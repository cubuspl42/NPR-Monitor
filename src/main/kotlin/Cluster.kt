import MessageType.POP
import MessageType.PUSH
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock


private const val MAX_QUEUE_SIZE = 10

class Cluster(
        private val thisNodeId: NodeId,
        private val distributedLock: DistributedLock,
        private val socketMap: Map<NodeId, MessageSocket>
) {
    private val messageQueue = ArrayBlockingQueue<Message>(8)

    var time = 0
        private set

    val distributedQueue = DistributedQueue<Int>(this, MAX_QUEUE_SIZE, Int::class.java)

    internal val lock = ReentrantLock()

    init {
        if (thisNodeId !in nodeAddresses.indices) throw AssertionError()

        socketMap.values.map { socket ->
            thread {
                while (true) {
                    messageQueue.put(socket.receive())
                }
            }
        }

        thread {
            println("Receiving loop...")
            while (true) {
                val message = messageQueue.take()
                handleMessage(message)
            }
        }
    }

    internal fun <T> synchronized(action: () -> T) = lock.withLock(action)

    internal fun send(nodeId: NodeId, message: Message) {
        ++time
        val socket = socketMap[nodeId]!!
        send(nodeId, socket, message, time)
    }

    internal fun broadcast(message: Message) {
        ++time
        socketMap.forEach { (nodeId, socket) ->
            send(nodeId, socket, message, time)
        }
    }

    private fun handleMessage(message: Message) = synchronized {
        println("Received message: $message")
        time = Math.max(time, message.timestamp) + 1
        when {
            message.type in setOf(PUSH, POP) -> distributedQueue.handleMessage(message)
            else -> distributedLock.handleMessage(message)
        }
    }

    private fun send(nodeId: NodeId, socket: MessageSocket, message: Message, timestamp: Int) {
        val filledMessage = message.copy(timestamp = timestamp, nodeId = thisNodeId)
        println("Sending message to $nodeId: $filledMessage")
        socket.send(filledMessage)
    }

    val size = nodeAddresses.size
}

private fun buildServerSocket(thisNodeId: NodeId): ServerSocket {
    val port = nodeAddresses[thisNodeId].port
    return ServerSocket(port)
}

private fun buildSocketMap(thisNodeId: NodeId, serverSocket: ServerSocket): Map<NodeId, Socket> {
    val lowerNodesSockets = connectToNodes(thisNodeId)
    val higherNodesSockets = acceptNodesConnections(thisNodeId, serverSocket)
    return (lowerNodesSockets + higherNodesSockets).toMap()
}

private fun connectToNodes(thisNodeId: NodeId): List<Pair<NodeId, Socket>> =
        (0 until thisNodeId).map { nodeId ->
            val socketAddress = nodeAddresses[nodeId]
            println("Connecting to $socketAddress...")
            nodeId to Socket(socketAddress.address, socketAddress.port)
        }

private fun acceptNodesConnections(thisNodeId: NodeId, serverSocket: ServerSocket): List<Pair<NodeId, Socket>> =
        (thisNodeId + 1 until nodeAddresses.size).map { nodeId ->
            val socket = serverSocket.accept()

            println("${socket.remoteSocketAddress} connected!")

            nodeId to socket
        }

fun Cluster(
        thisNodeId: NodeId,
        distributedLock: DistributedLock
): Cluster {
    val serverSocket = buildServerSocket(thisNodeId)

    println("Bound address: ${serverSocket.localSocketAddress}")

    val rawSocketMap = buildSocketMap(thisNodeId, serverSocket)

    println("Socket map:")
    rawSocketMap.forEach { nodeId, socket ->
        println("$nodeId: ${socket.localSocketAddress} -> ${socket.remoteSocketAddress}")
    }

    val socketMap = rawSocketMap.mapValues { MessageSocket(it.value) }

    return Cluster(thisNodeId, distributedLock, socketMap)
}
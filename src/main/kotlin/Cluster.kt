import MessageType.*
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

    private var responsesNeeded = 0

    private val allResponsesReceivedCond = lock.newCondition()

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
                receiveMessage(message)
            }
        }
    }

    internal fun <T> synchronized(action: () -> T) = lock.withLock(action)

    private fun send(nodeId: NodeId, message: Message) {
        ++time
        val filledMessage = message.copy(timestamp = time, nodeId = thisNodeId)
        val socket = socketMap[nodeId]!!
        send(nodeId, socket, filledMessage, time)
    }

    internal fun broadcast(message: Message) {
        ++time
        val filledMessage = message.copy(timestamp = time, nodeId = thisNodeId)
        socketMap.forEach { (nodeId, socket) ->
            send(nodeId, socket, filledMessage, time)
        }
        if (message.type.enableLoopback) {
            handleMessage(filledMessage)
        }
        if (message.type.requiresResponse) {
            waitForResponses()
        }
    }

    private fun waitForResponses() {
        responsesNeeded = size - 1
        while (responsesNeeded != 0) {
            allResponsesReceivedCond.await()
        }
    }

    private fun receiveMessage(message: Message) = synchronized {
        println("Received message: $message")
        time = Math.max(time, message.timestamp) + 1
        handleMessage(message)
        if (message.type.requiresResponse) {
            send(message.nodeId, Message(RESPONSE))
        }
    }

    private fun handleMessage(message: Message) {
        when {
            message.type == RESPONSE -> handleResponse()
            message.type in setOf(PUSH, POP) -> distributedQueue.handleMessage(message)
            else -> distributedLock.handleMessage(message)
        }
    }

    private fun handleResponse() {
        if (responsesNeeded <= 0) throw AssertionError()

        --responsesNeeded
        if (responsesNeeded == 0) {
            allResponsesReceivedCond.signal()
        }
    }

    private fun send(nodeId: NodeId, socket: MessageSocket, message: Message, timestamp: Int) {
        println("Sending message to $nodeId: $message")
        socket.send(message)
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
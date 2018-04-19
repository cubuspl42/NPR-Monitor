import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.net.ServerSocket
import java.net.Socket
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import kotlin.concurrent.thread


private val mapper = jacksonObjectMapper()

class Cluster(
        private val thisNodeId: NodeId,
        private val distributedLock: DistributedLock,
        private val socketMap: Map<NodeId, Socket>
) {
    private val messageQueue = ArrayBlockingQueue<Message>(8)

    private var time = 0

    init {
        if (thisNodeId !in nodeAddresses.indices) throw AssertionError()

        socketMap.values.map { socket ->
            thread {
                val scanner = Scanner(socket.getInputStream()).apply {
                    useDelimiter("\n")
                }
                while (true) {
                    val messageString = scanner.next()
                    val message = mapper.readValue<Message>(messageString)
                    messageQueue.put(message)
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

    internal fun send(nodeId: NodeId, message: Message) {
        if (nodeId == thisNodeId) {
            handleMessage(message)
        } else {
            val socket = socketMap[nodeId]!!
            send(socket, message)
        }
    }

    internal fun broadcast(message: Message) {
        handleMessage(message)
        socketMap.values.forEach { socket ->
            send(socket, message)
        }
    }

    private fun handleMessage(message: Message) {
        time = Math.max(time, message.timestamp) + 1
        distributedLock.handleMessage(message)
    }

    private fun send(socket: Socket, message: Message) {
        val filledMessage = message.copy(timestamp = ++time, nodeId = thisNodeId)
        println("Sending message to ${socket.remoteSocketAddress}: $filledMessage")
        socket.getOutputStream().run {
            write(mapper.writeValueAsBytes(filledMessage))
            write("\n".toByteArray())
        }
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

    val socketMap = buildSocketMap(thisNodeId, serverSocket)

    println("Socket map:")
    socketMap.forEach { nodeId, socket ->
        println("$nodeId: ${socket.localSocketAddress} -> ${socket.remoteSocketAddress}")
    }

    return Cluster(thisNodeId, distributedLock, socketMap)
}
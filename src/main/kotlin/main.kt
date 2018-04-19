import MessageType.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.serialization.Serializable
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadLocalRandom
import kotlin.concurrent.thread


typealias NodeId = Int

enum class MessageType {
    REQUEST,
    RESPONSE,
    RELEASE,
    WAIT,
    NOTIFY
}

@Serializable
data class Message(
        val type: MessageType,
        val timestamp: Int = -1,
        val nodeId: NodeId = -1,
        val conditionId: Int = -1
)

data class Request(
        val timestamp: Int,
        val nodeId: NodeId
) : Comparable<Request> {
    override fun compareTo(other: Request) = compareValuesBy(this, other,
            { it.timestamp },
            { it.nodeId }
    )
}

val nodeAddresses = listOf(
        InetSocketAddress("localhost", 5555),
        InetSocketAddress("localhost", 5556),
        InetSocketAddress("localhost", 5557),
        InetSocketAddress("localhost", 5558)
)

val mapper = jacksonObjectMapper()

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


class DistributedLock(
        private val thisNodeId: NodeId
) {
    private val cluster = Cluster(thisNodeId, this)

    private val semaphore = Semaphore(0)

    private val queue: Queue<Request> = PriorityQueue()

    private val conditions = mutableListOf<DistributedCondition>()

    private var responsesNeeded = 0

    fun acquire() {
        requestLock()
        sleep()
    }

    @Synchronized
    fun release() {
        cluster.broadcast(Message(RELEASE))
    }

    internal fun sleep() {
        semaphore.acquire()
    }

    private fun wakeup() {
        if (semaphore.availablePermits() != 0) throw AssertionError()
        semaphore.release()
    }

    @Synchronized
    private fun requestLock() {
        responsesNeeded = cluster.size
        cluster.broadcast(Message(REQUEST))
    }

    @Synchronized
    internal fun handleMessage(message: Message) {
        println("Received message: $message")

        when (message.type) {
            REQUEST -> {
                queue.add(Request(message.timestamp, message.nodeId))
                cluster.send(message.nodeId, Message(RESPONSE))
            }
            RESPONSE -> {
                if (responsesNeeded <= 0) throw AssertionError()

                --responsesNeeded
                if (responsesNeeded == 0) {
                    tryEnteringCriticalSection()
                }
            }
            RELEASE -> {
                onRelease(message)
            }
            WAIT -> {
                conditions[message.conditionId].queue.add(Request(message.timestamp, message.nodeId))
                onRelease(message)
            }
            NOTIFY -> {
                if (queue.firstOrNull()?.nodeId != message.nodeId) throw AssertionError()
                val request = conditions[message.conditionId].queue.poll()
                queue.add(request)
            }
        }

        println("Queue: $queue")
    }

    private fun onRelease(message: Message) {
        if (queue.firstOrNull()?.nodeId != message.nodeId) throw AssertionError()
        queue.remove()
        tryEnteringCriticalSection()
    }

    private fun tryEnteringCriticalSection() {
        if (queue.firstOrNull()?.nodeId == thisNodeId) {
            wakeup()
        }
    }
}

class DistributedCondition(
        private val distributedLock: DistributedLock,
        private val conditionId: Int,
        private val cluster: Cluster
) {
    internal val queue: Queue<Request> = PriorityQueue()

    fun wait_() {
        cluster.broadcast(Message(WAIT, conditionId = conditionId))
        distributedLock.sleep()
    }

    fun notify_() {
        cluster.broadcast(Message(NOTIFY, conditionId = conditionId))
    }
}

fun rand(range: ClosedRange<Int>) =
        ThreadLocalRandom.current().nextInt(range.endInclusive - range.start) + range.start

fun sleep(millis: Int) {
    println("Sleeping for $millis...")
    Thread.sleep(millis.toLong())
}

fun sleep(range: ClosedRange<Int>) {
    sleep(rand(range))
}

fun main(args: Array<String>) {
    val thisNodeId = args[0].toInt()

    val mutex = DistributedLock(thisNodeId)

    while (true) {
        sleep(1000..60000)
        println("About to acquire mutex...")
        mutex.acquire()
        println("Entered critical section!")
        sleep(1000..5000)
        println("About to release mutex...")
        mutex.release()
        println("Left critical section!")
    }
}
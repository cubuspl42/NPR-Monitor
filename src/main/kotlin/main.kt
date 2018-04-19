import MessageType.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.serialization.Serializable
import java.net.InetSocketAddress
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadLocalRandom


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
import State.*
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


enum class State {
    RELEASED,
    ACQUIRING_LOCK,
    INSIDE_CRITICAL_SECTION,
    WAITING

}

class DistributedLock(
        private val cluster: ClusterSocket,
        private val thisNodeId: NodeId
) {
    private val lock = ReentrantLock()

    private var state = RELEASED

    private val queue: Queue<Request> = PriorityQueue()

    private val conditions = mutableListOf<DistributedCondition>()

    private val firstInQueueCond = lock.newCondition()

    fun acquire() {
        lock.withLock {
            state = ACQUIRING_LOCK
        }
        cluster.broadcast(Message(MessageType.REQUEST))
        lock.withLock {
            ensureFirstInQueue()
            state = INSIDE_CRITICAL_SECTION
            println("Lock acquired")
        }
    }

    private fun ensureFirstInQueue() {
        while (queue.firstOrNull()?.nodeId != thisNodeId) {
            firstInQueueCond.await()
        }
    }

    fun release() = lock.withLock {
        if (state != INSIDE_CRITICAL_SECTION) throw AssertionError()
        cluster.broadcast(Message(MessageType.RELEASE))
        println("Lock released")
        state = RELEASED
    }

    fun newCondition(): DistributedCondition {
        val conditionId = conditions.size
        return DistributedCondition(cluster, this, conditionId).also {
            conditions.add(it)
        }
    }

    internal fun await() = lock.withLock {
        state = WAITING
        ensureFirstInQueue()
        state = INSIDE_CRITICAL_SECTION
    }

    internal fun handleMessage(message: ClusterMessage) = lock.withLock {
        when (message.content.type) {
            MessageType.REQUEST -> {
                queue.add(Request(message.timestamp, message.nodeId))
            }
            MessageType.RELEASE -> {
                if (state == INSIDE_CRITICAL_SECTION) throw AssertionError()
                onRelease(message.nodeId)
            }
            MessageType.WAIT -> {
                if ((message.nodeId != thisNodeId && state == INSIDE_CRITICAL_SECTION) ||
                        (message.nodeId == thisNodeId && state != INSIDE_CRITICAL_SECTION)) throw AssertionError()
                val condition = conditions[message.content.conditionId]
                condition.waitQueue.add(Request(message.timestamp, message.nodeId))
                onRelease(message.nodeId)
            }
            MessageType.NOTIFY -> {
                if ((message.nodeId != thisNodeId && state == INSIDE_CRITICAL_SECTION) ||
                        (message.nodeId == thisNodeId && state != INSIDE_CRITICAL_SECTION)) throw AssertionError()
                val condition = conditions[message.content.conditionId]
                condition.waitQueue.poll()?.let { waitRequest ->
                    queue.add(Request(message.timestamp, waitRequest.nodeId))
                }
            }
            else -> throw AssertionError()
        }

        println("Queue: $queue")
    }

    private fun onRelease(nodeId: NodeId) {
        if (queue.firstOrNull()?.nodeId == nodeId) {
            val request = queue.remove()
        } else {
            if (queue.none { it.nodeId == nodeId }) throw AssertionError()
            queue.removeIf { it.nodeId == nodeId }
        }

        firstInQueueCond.signal()
    }
}

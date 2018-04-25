import State.*
import java.util.*


enum class State {
    RELEASED,
    ACQUIRING_LOCK,
    INSIDE_CRITICAL_SECTION,
    WAITING

}

class DistributedLock(
        internal val thisNodeId: NodeId
) {
    private var state = RELEASED

    private val cluster = Cluster(thisNodeId, this)

    val distributedQueue = cluster.distributedQueue

    private val queue: Queue<Request> = PriorityQueue()

    private val conditions = mutableListOf<DistributedCondition>()

    private val firstInQueueCond = cluster.lock.newCondition()

    fun acquire() {
        cluster.synchronized {
            state = ACQUIRING_LOCK
            cluster.broadcast(Message(MessageType.REQUEST))
            ensureFirstInQueue()
            state = INSIDE_CRITICAL_SECTION
        }
        println("Lock acquired")
    }

    private fun ensureFirstInQueue() {
        while (queue.firstOrNull()?.nodeId != thisNodeId) {
            firstInQueueCond.await()
        }
    }

    fun release() = cluster.synchronized {
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

    internal fun await() {
        state = WAITING
        ensureFirstInQueue()
        state = INSIDE_CRITICAL_SECTION
    }

    internal fun handleMessage(message: Message) {
        when (message.type) {
            MessageType.REQUEST -> {
                queue.add(Request(message.timestamp, message.nodeId))
            }
            MessageType.RELEASE -> {
//                if (state == INSIDE_CRITICAL_SECTION) throw AssertionError() // FIXME: loopback
                onRelease(message.nodeId)
            }
            MessageType.WAIT -> {
//                if (state == INSIDE_CRITICAL_SECTION) throw AssertionError() // FIXME: loopback
                val condition = conditions[message.conditionId]
                condition.waitQueue.add(Request(message.timestamp, message.nodeId))
                onRelease(message.nodeId)
            }
            MessageType.NOTIFY -> {
//                if (state == INSIDE_CRITICAL_SECTION) throw AssertionError() // FIXME: loopback
                val condition = conditions[message.conditionId]
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

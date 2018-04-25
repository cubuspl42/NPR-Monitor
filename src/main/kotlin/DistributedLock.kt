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

//    private val semaphore = BinarySemaphore()

    private val queue: Queue<Request> = PriorityQueue()

    private val conditions = mutableListOf<DistributedCondition>()

    private var responsesNeeded = 0

    private val allResponsesReceivedCond = cluster.lock.newCondition()

    private val firstInQueueCond = cluster.lock.newCondition()

    fun acquire() {
        cluster.synchronized {
            state = ACQUIRING_LOCK
            queue.add(Request(cluster.time + 1, thisNodeId))
            cluster.broadcast(Message(MessageType.REQUEST))
            waitForResponses()
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

    private fun waitForResponses() {
        responsesNeeded = cluster.size - 1
        while (responsesNeeded != 0) {
            allResponsesReceivedCond.await()
        }
    }

    fun release() = cluster.synchronized {
        if (state != INSIDE_CRITICAL_SECTION) throw AssertionError()
        onRelease()
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
                cluster.send(message.nodeId, Message(MessageType.RESPONSE))
            }
            MessageType.RESPONSE -> {
                if (state !in setOf(ACQUIRING_LOCK)) throw AssertionError()
                if (responsesNeeded <= 0) throw AssertionError()

                --responsesNeeded
                if (responsesNeeded == 0) {
                    allResponsesReceivedCond.signal()
                }
            }
            MessageType.RELEASE -> {
                if (state == INSIDE_CRITICAL_SECTION) throw AssertionError()
                onRelease(message.nodeId)
            }
            MessageType.WAIT -> {
                if (state == INSIDE_CRITICAL_SECTION) throw AssertionError()
                conditions[message.conditionId].waitQueue.add(Request(message.timestamp, message.nodeId))
                onRelease(message.nodeId)
            }
            MessageType.NOTIFY -> {
                if (state == INSIDE_CRITICAL_SECTION) throw AssertionError()
                conditions[message.conditionId].notifyQueue.add(Request(message.timestamp, message.nodeId))
            }
            else -> throw AssertionError()
        }

        println("Queue: $queue")
    }

    internal fun onRelease() {
        onRelease(thisNodeId)
        if (queue.any { it.nodeId == thisNodeId }) throw AssertionError()
        state = RELEASED
    }

    private fun onRelease(nodeId: NodeId) {
        if (queue.firstOrNull()?.nodeId == nodeId) {
            val request = queue.remove()
            processNotifyRequests(request.timestamp)
        } else {
            if (queue.none { it.nodeId == nodeId }) throw AssertionError()
            queue.removeIf { it.nodeId == nodeId }
        }

        firstInQueueCond.signal()
    }

    private fun processNotifyRequests(timestamp: Int) {
        println("Processing NOTIFY requests...")
        for (condition in conditions) {
            val notifyQueue = condition.notifyQueue
            while (notifyQueue.isNotEmpty() && notifyQueue.first().timestamp < timestamp) {
                val notifyRequest = notifyQueue.remove()
                println("Processing NOTIFY request: $notifyRequest")
                condition.waitQueue.poll()?.let { waitRequest ->
                    println("Pushing WAIT request to queue: $waitRequest")
                    queue.add(Request(notifyRequest.timestamp, waitRequest.nodeId))
                    firstInQueueCond.signal()
                }
            }
        }
    }
}

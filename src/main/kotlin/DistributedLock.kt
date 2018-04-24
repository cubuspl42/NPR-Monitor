import java.util.*


class DistributedLock(
        internal val thisNodeId: NodeId
) {
    private val cluster = Cluster(thisNodeId, this)

    val distributedQueue = cluster.distributedQueue

    private val semaphore = BinarySemaphore()

    private val queue: Queue<Request> = PriorityQueue()

    private val conditions = mutableListOf<DistributedCondition>()

    private var responsesNeeded = 0

    fun acquire() {
        cluster.synchronized {
            responsesNeeded = cluster.size - 1
            queue.add(Request(cluster.time + 1, thisNodeId))
            cluster.broadcast(Message(MessageType.REQUEST))
        }
        semaphore.await()
        println("Lock acquired")
    }

    fun release() = cluster.synchronized {
        onRelease()
        cluster.broadcast(Message(MessageType.RELEASE))
        println("Lock released")
    }

    fun newCondition(): DistributedCondition {
        val conditionId = conditions.size
        return DistributedCondition(cluster, this, semaphore, conditionId).also {
            conditions.add(it)
        }
    }

    internal fun handleMessage(message: Message) {
        when (message.type) {
            MessageType.REQUEST -> {
                queue.add(Request(message.timestamp, message.nodeId))
                cluster.send(message.nodeId, Message(MessageType.RESPONSE))
            }
            MessageType.RESPONSE -> {
                if (responsesNeeded <= 0) throw AssertionError()

                --responsesNeeded
                if (responsesNeeded == 0) {
                    tryEnteringCriticalSection()
                }
            }
            MessageType.RELEASE -> {
                onRelease(message.nodeId)
            }
            MessageType.WAIT -> {
                conditions[message.conditionId].waitQueue.add(Request(message.timestamp, message.nodeId))
                onRelease(message.nodeId)
            }
            MessageType.NOTIFY -> {
                conditions[message.conditionId].notifyQueue.add(Request(message.timestamp, message.nodeId))
            }
            else -> throw AssertionError()
        }

        println("Queue: $queue")
    }

    internal fun onRelease() {
        onRelease(thisNodeId)
        if (queue.any { it.nodeId == thisNodeId }) throw AssertionError()
    }

    private fun onRelease(nodeId: NodeId) {
        if (queue.firstOrNull()?.nodeId == nodeId) {
            val request = queue.remove()
            processNotifyRequests(request.timestamp)
        } else {
            if (queue.none { it.nodeId == nodeId }) throw AssertionError()
            queue.removeIf { it.nodeId == nodeId }
        }

        tryEnteringCriticalSection()
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
                }
            }
        }
    }

    private fun tryEnteringCriticalSection() {
        if (queue.firstOrNull()?.nodeId == thisNodeId && responsesNeeded == 0) {
            enterCriticalSection()
        }
    }

    private fun enterCriticalSection() = semaphore.signal()
}

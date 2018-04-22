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
        requestLock()
        semaphore.await()
        println("Lock acquired")
    }

    @Synchronized
    fun release() {
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

    @Synchronized
    private fun requestLock() {
        responsesNeeded = cluster.size - 1
        queue.add(Request(cluster.time + 1, thisNodeId))
        cluster.broadcast(Message(MessageType.REQUEST))
    }

    @Synchronized
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
                conditions[message.conditionId].queue.add(Request(message.timestamp, message.nodeId))
                onRelease(message.nodeId)
            }
            MessageType.NOTIFY -> {
                // TODO: Queue notify requests
                conditions[message.conditionId].queue.poll()?.let { queue.add(it) }
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
        if (!queue.any { it.nodeId == nodeId }) throw AssertionError()
        queue.removeIf { it.nodeId == nodeId }
        tryEnteringCriticalSection()
    }

    private fun tryEnteringCriticalSection() {
        if (queue.firstOrNull()?.nodeId == thisNodeId && responsesNeeded == 0) {
            enterCriticalSection()
        }
    }

    private fun enterCriticalSection() = semaphore.signal()
}

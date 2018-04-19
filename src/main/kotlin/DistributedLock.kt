import java.util.*


class DistributedLock(
        private val thisNodeId: NodeId
) {
    private val cluster = Cluster(thisNodeId, this)

    private val semaphore = BinarySemaphore()

    private val queue: Queue<Request> = PriorityQueue()

    private val conditions = mutableListOf<DistributedCondition>()

    private var responsesNeeded = 0

    fun acquire() {
        requestLock()
        semaphore.await()
    }

    @Synchronized
    fun release() {
        cluster.broadcast(Message(MessageType.RELEASE))
    }

    fun newCondition(): DistributedCondition {
        val conditionId = conditions.size
        return DistributedCondition(cluster, semaphore, conditionId).also {
            conditions.add(it)
        }
    }

    @Synchronized
    private fun requestLock() {
        responsesNeeded = cluster.size
        cluster.broadcast(Message(MessageType.REQUEST))
    }

    @Synchronized
    internal fun handleMessage(message: Message) {
        println("Received message: $message")

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
                onRelease(message)
            }
            MessageType.WAIT -> {
                conditions[message.conditionId].queue.add(Request(message.timestamp, message.nodeId))
                onRelease(message)
            }
            MessageType.NOTIFY -> {
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
            semaphore.signal()
        }
    }
}

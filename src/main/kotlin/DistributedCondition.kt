import java.util.*


class DistributedCondition(
        private val cluster: Cluster,
        private val distributedLock: DistributedLock,
        private val semaphore: BinarySemaphore,
        private val conditionId: Int
) {
    internal val queue: Queue<Request> = PriorityQueue()

    fun await() {
        queue.add(Request(cluster.time + 1, distributedLock.thisNodeId))
        distributedLock.onRelease()
        cluster.broadcast(Message(MessageType.WAIT, conditionId = conditionId))
        println("Waiting for condition $conditionId")
        semaphore.await()
        println("Condition $conditionId finally met; reentering critical section")
    }

    fun signal() {
        println("Signaling condition $conditionId")
        cluster.broadcast(Message(MessageType.NOTIFY, conditionId = conditionId))
    }
}

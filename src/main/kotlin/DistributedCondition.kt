import java.util.*


class DistributedCondition(
        private val cluster: Cluster,
        private val semaphore: BinarySemaphore,
        private val conditionId: Int
) {
    internal val queue: Queue<Request> = PriorityQueue()

    fun await() {
        cluster.broadcast(Message(MessageType.WAIT, conditionId = conditionId))
        semaphore.await()
    }

    fun signal() {
        cluster.broadcast(Message(MessageType.NOTIFY, conditionId = conditionId))
    }
}

import java.util.*


class DistributedCondition(
        private val distributedLock: DistributedLock,
        private val conditionId: Int,
        private val cluster: Cluster
) {
    internal val queue: Queue<Request> = PriorityQueue()

    fun await() {
        cluster.broadcast(Message(MessageType.WAIT, conditionId = conditionId))
        distributedLock.sleep()
    }

    fun signal() {
        cluster.broadcast(Message(MessageType.NOTIFY, conditionId = conditionId))
    }
}

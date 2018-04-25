import java.util.*


class DistributedCondition(
        private val cluster: Cluster,
        private val distributedLock: DistributedLock,
        private val conditionId: Int
) {
    internal val waitQueue: Queue<Request> = PriorityQueue()

    fun await() {
        cluster.synchronized {
            cluster.broadcast(Message(MessageType.WAIT, conditionId = conditionId)) // TODO: separate wait & release
            println("Waiting for condition $conditionId")
            distributedLock.await()
            println("Condition $conditionId finally met; reentering critical section")
        }
    }

    fun signal() = cluster.synchronized {
        println("Signaling condition $conditionId")
        cluster.broadcast(Message(MessageType.NOTIFY, conditionId = conditionId))
    }
}

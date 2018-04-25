import java.util.*


class DistributedCondition(
        private val cluster: Cluster,
        private val distributedLock: DistributedLock,
        private val conditionId: Int
) {
    internal val waitQueue: Queue<Request> = PriorityQueue()

    internal val notifyQueue: Queue<Request> = PriorityQueue() // TODO: remove

    fun await() {
        cluster.synchronized {
            waitQueue.add(Request(cluster.time + 1, distributedLock.thisNodeId)) // FIXME
            distributedLock.onRelease()
            cluster.broadcast(Message(MessageType.WAIT, conditionId = conditionId)) // TODO: separate wait & release
            // TODO: wait for responses
            println("Waiting for condition $conditionId")
            distributedLock.await()
            println("Condition $conditionId finally met; reentering critical section")
        }
    }

    fun signal() = cluster.synchronized {
        println("Signaling condition $conditionId")
        notifyQueue.add(Request(cluster.time + 1, distributedLock.thisNodeId)) // FIXME
        cluster.broadcast(Message(MessageType.NOTIFY, conditionId = conditionId))
        // TODO: wait for responses
    }
}

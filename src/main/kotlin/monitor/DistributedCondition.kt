package monitor

import java.util.*


class DistributedCondition(
        private val clusterSocket: ClusterSocket,
        private val distributedLock: DistributedLock,
        private val conditionId: Int
) {
    internal val waitQueue: Queue<Request> = PriorityQueue()

    fun await() {
        clusterSocket.broadcast(Message(MessageType.WAIT, conditionId = conditionId)) // TODO: separate wait & release
        println("Waiting for condition $conditionId")
        distributedLock.await()
        println("Condition $conditionId finally met; reentering critical section")

    }

    fun signal() {
        println("Signaling condition $conditionId")
        clusterSocket.broadcast(Message(MessageType.NOTIFY, conditionId = conditionId))
    }
}

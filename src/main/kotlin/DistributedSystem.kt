import kotlin.concurrent.thread

class DistributedSystem(thisNodeId: NodeId) {
    private val clusterSocket = Cluster(thisNodeId)

    val distributedLock = DistributedLock(clusterSocket, thisNodeId)

    val distributedQueue = DistributedQueue(clusterSocket, 10, Int::class.java)

    init {
        thread {
            while (true) {
                val message = clusterSocket.receive()
                handleMessage(message)
            }
        }
    }

    private fun handleMessage(message: ClusterMessage) {
        println("Handling message: $message")
        when (message.content.type) {
            MessageType.POP, MessageType.PUSH -> distributedQueue.handleMessage(message)
            else -> distributedLock.handleMessage(message)
        }
    }
}

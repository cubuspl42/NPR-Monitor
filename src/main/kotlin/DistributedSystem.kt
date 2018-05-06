import java.net.InetSocketAddress
import kotlin.concurrent.thread


private const val MAX_QUEUE_SIZE = 10

class DistributedSystem(thisNodeId: NodeId, tcpClusterConfig: List<InetSocketAddress>) {
    private val clusterSocket = tcpClusterSocket(thisNodeId, tcpClusterConfig)

    val distributedLock = DistributedLock(clusterSocket, thisNodeId)

    val distributedQueue = DistributedQueue(clusterSocket, MAX_QUEUE_SIZE, Int::class.java)

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

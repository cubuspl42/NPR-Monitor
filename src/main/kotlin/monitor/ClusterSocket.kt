package monitor

import monitor.MessageType.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock


data class ClusterMessage(
        val timestamp: Int,
        val nodeId: NodeId,
        val content: Message
)

class ClusterSocket(
        private val thisNodeId: NodeId,
        private val socketMap: Map<NodeId, MessageSocket>
) {
    private val messageQueue = ArrayBlockingQueue<ClusterMessage>(8)

    private var time = 0

    private val lock = ReentrantLock()

    private var responsesNeeded = 0

    private val allResponsesReceivedCond = lock.newCondition()

    init {
        socketMap.values.map { socket ->
            thread {
                while (true) {
                    val message = socket.receive()
                    receiveMessage(message)
                }
            }
        }
    }

    fun receive(): ClusterMessage {
        while (true) {
            val message = messageQueue.take()!!
            if (message.content.type == RESPONSE) {
                handleResponse()
            } else return message
        }
    }

    private fun send(nodeId: NodeId, message: Message) {
        ++time
        val filledMessage = ClusterMessage(time, thisNodeId, message)
        val socket = socketMap[nodeId]!!
        send(nodeId, socket, filledMessage)
    }

    fun broadcast(message: Message) = lock.withLock {
        ++time
        val filledMessage = ClusterMessage(time, thisNodeId, message)
        socketMap.forEach { (nodeId, socket) ->
            send(nodeId, socket, filledMessage)
        }
        if (message.type.enableLoopback) {
            enqueueMessage(filledMessage)
        }
        if (message.type.requiresResponse) {
            waitForResponses()
        }
    }

    private fun waitForResponses() {
        if (responsesNeeded != 0) throw AssertionError()
        responsesNeeded = clusterSize - 1
        while (responsesNeeded != 0) {
            allResponsesReceivedCond.await()
        }
    }

    private fun receiveMessage(message: ClusterMessage) = lock.withLock {
        println("Received message: $message")
        time = Math.max(time, message.timestamp) + 1

        enqueueMessage(message)

        if (message.content.type.requiresResponse) {
            send(message.nodeId, Message(RESPONSE))
        }
    }

    private fun enqueueMessage(message: ClusterMessage) {
        messageQueue.put(message)
    }

    private fun handleResponse() = lock.withLock {
        if (responsesNeeded <= 0) throw AssertionError()

        --responsesNeeded
        if (responsesNeeded == 0) {
            allResponsesReceivedCond.signal()
        }
    }

    private fun send(nodeId: NodeId, socket: MessageSocket, message: ClusterMessage) {
        println("Sending message to $nodeId: $message")
        socket.send(message)
    }

    private val clusterSize = socketMap.size + 1
}

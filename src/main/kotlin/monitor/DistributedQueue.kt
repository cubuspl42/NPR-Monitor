package monitor

import monitor.MessageType.POP
import monitor.MessageType.PUSH
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.util.*


private val mapper = jacksonObjectMapper()

class DistributedQueue<T>(
        private val clusterSocket: ClusterSocket,
        private val maxSize: Int,
        private val valueType: Class<T>
) {
    private val queue: Queue<T> = LinkedList()

    fun add(element: T) {
        queue.add(element)
        val value = mapper.writeValueAsString(element)
        clusterSocket.broadcast(Message(PUSH, value = value))
    }

    fun poll(): T {
        val value = queue.poll()
        clusterSocket.broadcast(Message(POP))
        return value
    }

    fun isEmpty() = queue.isEmpty()

    fun isFull() = queue.size == maxSize

    internal fun handleMessage(message: ClusterMessage) {
        // FIXME: PUSH order
        when (message.content.type) {
            PUSH -> {
                val element = mapper.readValue<T>(message.content.value, valueType)
                queue.add(element)
            }
            POP -> {
                queue.remove()
            }
            else -> throw AssertionError()
        }
        println("monitor.DistributedQueue: $queue")
    }
}

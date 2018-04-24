import MessageType.POP
import MessageType.PUSH
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.util.*


private val mapper = jacksonObjectMapper()

class DistributedQueue<T>(
        private val cluster: Cluster,
        private val maxSize: Int,
        private val valueType: Class<T>
) {
    private val queue: Queue<T> = LinkedList()

    fun add(element: T) {
        queue.add(element)
        val value = mapper.writeValueAsString(element)
        cluster.broadcast(Message(PUSH, value = value))
    }

    fun poll(): T {
        val value = queue.poll()
        cluster.broadcast(Message(POP))
        return value
    }

    fun isEmpty() = queue.isEmpty()

    fun isFull() = queue.size == maxSize

    internal fun handleMessage(message: Message) {
        // FIXME: PUSH order
        when (message.type) {
            PUSH -> {
                val element = mapper.readValue<T>(message.value, valueType)
                queue.add(element)
            }
            POP -> {
                queue.remove()
            }
            else -> throw AssertionError()
        }
        println("DistributedQueue: $queue")
    }
}

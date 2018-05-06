import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.net.Socket
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


private val mapper = jacksonObjectMapper()

class MessageSocket(private val socket: Socket) {
    private val receiveLock = ReentrantLock()

    private val sendLock = ReentrantLock()

    private val scanner = Scanner(socket.getInputStream()).apply {
        useDelimiter("\n")
    }

    fun receive() = receiveLock.withLock {
        mapper.readValue<ClusterMessage>(scanner.next()) // TODO: fix exception type
    }

    fun send(message: ClusterMessage) = sendLock.withLock {
        socket.getOutputStream().run {
            write(mapper.writeValueAsBytes(message))
            write("\n".toByteArray())
        }
    }
}

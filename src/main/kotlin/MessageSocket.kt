import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.net.Socket
import java.util.*


private val mapper = jacksonObjectMapper()

class MessageSocket(private val socket: Socket) {
    private val scanner = Scanner(socket.getInputStream()).apply {
        useDelimiter("\n")
    }

    fun receive() =
            mapper.readValue<Message>(scanner.next())

    fun send(message: Message) {
        socket.getOutputStream().run {
            write(mapper.writeValueAsBytes(message))
            write("\n".toByteArray())
        }
    }
}

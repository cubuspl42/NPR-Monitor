typealias NodeId = Int

enum class MessageType(
        val requiresResponse: Boolean = false,
        val enableLoopback: Boolean = false
) {
    REQUEST(requiresResponse = true, enableLoopback = true),
    RESPONSE,
    RELEASE(enableLoopback = true),
    WAIT(requiresResponse = true, enableLoopback = true),
    NOTIFY(requiresResponse = true, enableLoopback = true),
    PUSH,
    POP
}

data class Message(
        val type: MessageType,
        val timestamp: Int = -1,
        val nodeId: NodeId = -1,
        val conditionId: Int = -1,
        val value: String = ""
)

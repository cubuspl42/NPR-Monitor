typealias NodeId = Int

enum class MessageType {
    REQUEST,
    RESPONSE,
    RELEASE,
    WAIT,
    NOTIFY,
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

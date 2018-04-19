data class Request(
        val timestamp: Int,
        val nodeId: NodeId
) : Comparable<Request> {
    override fun compareTo(other: Request) = compareValuesBy(this, other,
            { it.timestamp },
            { it.nodeId }
    )
}

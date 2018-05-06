package monitor

import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket

private fun buildSocketMap(thisNodeId: NodeId, clusterConfig: List<InetSocketAddress>): Map<NodeId, Socket> {
    val serverSocket = ServerSocket(clusterConfig[thisNodeId].port)
    println("Bound address: ${serverSocket.localSocketAddress}")
    val lowerNodesSockets = connectToNodes(clusterConfig.subList(0, thisNodeId))
    val higherNodesSockets = acceptNodesConnections(thisNodeId + 1 until clusterConfig.size, serverSocket)
    return (lowerNodesSockets + higherNodesSockets).toMap()
}

private fun connectToNodes(addresses: List<InetSocketAddress>): List<Pair<NodeId, Socket>> =
        addresses.mapIndexed { nodeId, socketAddress ->
            println("Connecting to $socketAddress...")
            nodeId to Socket(socketAddress.address, socketAddress.port)
        }

private fun acceptNodesConnections(nodeIdRange: IntRange, serverSocket: ServerSocket): List<Pair<NodeId, Socket>> =
        nodeIdRange.map { nodeId ->
            val socket = serverSocket.accept()
            println("${socket.remoteSocketAddress} connected!")
            nodeId to socket
        }

fun tcpClusterSocket(thisNodeId: NodeId, clusterConfig: List<InetSocketAddress>): ClusterSocket {
    val rawSocketMap = buildSocketMap(thisNodeId, clusterConfig)

    println("Socket map:")
    rawSocketMap.forEach { nodeId, socket ->
        println("$nodeId: ${socket.localSocketAddress} -> ${socket.remoteSocketAddress}")
    }

    val socketMap = rawSocketMap.mapValues { MessageSocket(it.value) }

    return ClusterSocket(thisNodeId, socketMap)
}

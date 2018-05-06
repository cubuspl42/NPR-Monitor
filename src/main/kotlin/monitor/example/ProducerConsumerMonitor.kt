package monitor.example

import monitor.DistributedSystem
import monitor.NodeId
import monitor.tcpClusterSocket
import java.net.InetSocketAddress
import java.util.concurrent.ThreadLocalRandom


private val tcpClusterConfig = listOf(
        InetSocketAddress("localhost", 5555),
        InetSocketAddress("localhost", 5556),
        InetSocketAddress("localhost", 5557),
        InetSocketAddress("localhost", 5558)
)

private fun rand(range: ClosedRange<Int>) =
        ThreadLocalRandom.current().nextInt(range.endInclusive - range.start + 1) + range.start

private fun sleep(millis: Int) {
    println("Sleeping for $millis...")
    Thread.sleep(millis.toLong())
}

private fun sleep(range: ClosedRange<Int>) {
    sleep(rand(range))
}

class ProducerConsumerMonitor(thisNodeId: NodeId) {
    private val clusterSocket = tcpClusterSocket(thisNodeId, tcpClusterConfig)

    private val distributedSystem = DistributedSystem(clusterSocket, thisNodeId)

    private val lock = distributedSystem.distributedLock

    private val queue = distributedSystem.distributedQueue

    private val queueNonFull = lock.newCondition()

    private val queueNonEmpty = lock.newCondition()

    fun produce(value: Int) {
        lock.acquire()

        println("produce: Entered critical section")

        while (queue.isFull()) {
            queueNonFull.await()
        }

        queue.add(value)

        queueNonEmpty.signal()

        lock.release()
    }

    fun consume(): Int {
        lock.acquire()

        println("consume: Entered critical section")

        while (queue.isEmpty()) {
            queueNonEmpty.await()
        }

        val value = queue.poll()

        queueNonFull.signal()

        lock.release()

        return value
    }
}

fun producerThread(monitor: ProducerConsumerMonitor, iterationCount: Int) {
    repeat(iterationCount) {
        sleep(1000..5000)
        val value = rand(0..100)
        println("Producing value: $value")
        monitor.produce(value)
    }
}

fun consumerThread(monitor: ProducerConsumerMonitor, iterationCount: Int) {
    repeat(iterationCount) {
        sleep(1000..5000)
        val value = monitor.consume()
        println("Consumed value: $value")
    }
}

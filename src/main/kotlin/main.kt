import java.net.InetSocketAddress
import java.util.concurrent.ThreadLocalRandom


val nodeAddresses = listOf(
        InetSocketAddress("localhost", 5555),
        InetSocketAddress("localhost", 5556),
        InetSocketAddress("localhost", 5557),
        InetSocketAddress("localhost", 5558)
)

fun rand(range: ClosedRange<Int>) =
        ThreadLocalRandom.current().nextInt(range.endInclusive - range.start + 1) + range.start

fun sleep(millis: Int) {
    println("Sleeping for $millis...")
    Thread.sleep(millis.toLong())
}

fun sleep(range: ClosedRange<Int>) {
    sleep(rand(range))
}

class ProducerConsumer(thisNodeId: NodeId) {
    private val lock = DistributedLock(thisNodeId)

    private val queue = lock.distributedQueue

    private val queueNonFull = lock.newCondition()

    private val queueNonEmpty = lock.newCondition()

    private fun p() = false

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

fun main(args: Array<String>) {
    val thisNodeId = args[0].toInt()

//    val mutex = DistributedLock(thisNodeId)
//    while (true) {
//        sleep(1000..60000)
//        println("About to acquire mutex...")
//        mutex.acquire()
//        println("Entered critical section!")
//        sleep(1000..5000)
//        println("About to release mutex...")
//        mutex.release()
//        println("Left critical section!")
//    }

    val p = ProducerConsumer(thisNodeId)

    when (rand(0..1)) {
        0 -> {
            while (true) {
                sleep(1000..5000)
                val value = rand(0..100)
                println("Producing value: $value")
                p.produce(value)
            }
        }
        else -> {
            while (true) {
                sleep(1000..5000)
                val value = p.consume()
                println("Consumed value: $value")
            }
        }
    }
}

import java.net.InetSocketAddress
import java.util.concurrent.ThreadLocalRandom


val nodeAddresses = listOf(
        InetSocketAddress("localhost", 5555),
        InetSocketAddress("localhost", 5556),
        InetSocketAddress("localhost", 5557),
        InetSocketAddress("localhost", 5558)
)

fun rand(range: ClosedRange<Int>) =
        ThreadLocalRandom.current().nextInt(range.endInclusive - range.start) + range.start

fun sleep(millis: Int) {
    println("Sleeping for $millis...")
    Thread.sleep(millis.toLong())
}

fun sleep(range: ClosedRange<Int>) {
    sleep(rand(range))
}

fun main(args: Array<String>) {
    val thisNodeId = args[0].toInt()

    val mutex = DistributedLock(thisNodeId)

    while (true) {
        sleep(1000..60000)
        println("About to acquire mutex...")
        mutex.acquire()
        println("Entered critical section!")
        sleep(1000..5000)
        println("About to release mutex...")
        mutex.release()
        println("Left critical section!")
    }
}

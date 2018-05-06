package monitor.example


private const val ITERATION_COUNT = 10000

fun main(args: Array<String>) {
    val thisNodeId = args[0].toInt()

    val monitor = ProducerConsumerMonitor(thisNodeId)

    when {
        thisNodeId < 2 -> {
            producerThread(monitor, ITERATION_COUNT)
        }
        else -> {
            consumerThread(monitor, ITERATION_COUNT)
        }
    }
}

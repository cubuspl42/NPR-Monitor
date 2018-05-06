package monitor

import monitor.example.ProducerConsumerMonitor
import monitor.example.consumerThread
import monitor.example.producerThread
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.concurrent.thread

private const val ITERATION_COUNT = 25

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SystemTest {
    @Test
    fun systemTest() {
        val threads = mutableListOf<Thread>()

        threads.add(thread { producerThread(ProducerConsumerMonitor(0), ITERATION_COUNT) })
        Thread.sleep(100)
        threads.add(thread { producerThread(ProducerConsumerMonitor(1), ITERATION_COUNT) })
        Thread.sleep(100)
        threads.add(thread { consumerThread(ProducerConsumerMonitor(2), ITERATION_COUNT) })
        Thread.sleep(100)
        threads.add(thread { consumerThread(ProducerConsumerMonitor(3), ITERATION_COUNT) })
        Thread.sleep(100)

        threads.forEach { it.join() }
    }
}

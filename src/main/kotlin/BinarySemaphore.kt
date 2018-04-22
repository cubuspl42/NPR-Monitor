import java.util.concurrent.Semaphore

class BinarySemaphore {
    private val semaphore = Semaphore(0)

    fun await() {
        semaphore.acquire()
    }

    fun signal() {
        val availablePermits = semaphore.availablePermits()
        if (availablePermits != 0) throw AssertionError()
        semaphore.release()
    }
}

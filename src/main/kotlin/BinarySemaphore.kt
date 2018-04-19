import java.util.concurrent.Semaphore

class BinarySemaphore {
    private val semaphore = Semaphore(0)

    fun await() {
        semaphore.acquire()
    }

    fun signal() {
        if (semaphore.availablePermits() != 0) throw AssertionError()
        semaphore.release()
    }
}

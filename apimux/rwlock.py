import threading


class ReadWriteLock:
    """Allows multiple readers while blocking the writing"""

    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0

    def acquire_read(self):
        with self._read_ready:
            self._readers += 1

    def release_read(self):
        with self._read_ready:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notifyAll()

    def acquire_write(self):
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()

    def release_write(self):
        self._read_ready.release()

    def __enter__(self):
        self.acquire_read()

    def __exit__(self, type, value, traceback):
        self.release_read()

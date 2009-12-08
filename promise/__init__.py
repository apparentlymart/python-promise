
from threading import Condition
from threading import RLock
from collections import deque

from promise.threadpool import ThreadPool

thread_pool = ThreadPool()
satisfied_cv = Condition()
outstanding_count = 0
outstanding_lock = RLock()


def _inject(promise):
    def func():
        global outstanding_count, outstanding_lock, satisfied_cv
        
        try:
            result = promise.func()
        except Exception as ex:
            satisfied_cv.acquire()
            promise.resolved = True
            promise._error = ex
            with outstanding_lock:
                outstanding_count = outstanding_count - 1
            satisfied_cv.notifyAll()
            satisfied_cv.release()
        else:
            satisfied_cv.acquire()
            promise.resolved = True
            promise._result = result
            with outstanding_lock:
                outstanding_count = outstanding_count - 1
            satisfied_cv.notifyAll()
            satisfied_cv.release()

    global outstanding_count, outstanding_lock, thread_pool

    with outstanding_lock:
        outstanding_count = outstanding_count + 1

    thread_pool.add_job(func)


def resolve_all():
    # Block until everything's finished
    global satisfied_cv, outstanding_count
    satisfied_cv.acquire()
    while outstanding_count > 0:
        satisfied_cv.wait()
    satisfied_cv.release()


class Promise(object):
    def __init__(self, func):
        self.func = func
        self._result = None
        self._error = None
        self.resolved = False
        _inject(self)

    def __call__(self):
        return self.result

    @property
    def result(self):
        global satisfied_cv
        # Block until the result is actually available.
        satisfied_cv.acquire()
        while not self.resolved:
            satisfied_cv.wait()
        satisfied_cv.release()
        if self._error is not None:
            # Re-raise exception in the calling thread
            raise self._error
        else:
            return self._result



import threading
from threading import Thread
from threading import RLock
from threading import Semaphore
from collections import deque

class ThreadPool(object):

    def __init__(self, thread_count = 10):
        self.resize_lock = RLock()
        self.debug_print_lock = RLock()
        self.job_semaphore = Semaphore(0)
        self.queue = deque()
        self.actual_thread_count = 0
        self.desired_thread_count = 0

        self.start_threads(thread_count)

    def __del__(self):
        self.terminate()

    def _debug(self, message):
        with (self.debug_print_lock):
            #print "ThreadPool: %s" % message
            pass

    def start_threads(self, thread_count):
        self._debug("Starting %i threads" % thread_count)
        with self.resize_lock:
            self.desired_thread_count += thread_count
            for i in range(thread_count):
                t = WorkerThread(self)
                t.start()
                self.actual_thread_count += 1

    def stop_threads(self, thread_count):
        self._debug("Stopping %i threads" % thread_count)
        with self.resize_lock:
            self.desired_thread_count -= 1
            self.add_job(None)

    def terminate(self):
        # Stop all of our threads as quickly as possible
        self._debug("Terminating") 
        with self.resize_lock:
            while self.desired_thread_count > 0:
                self.stop_threads(self.actual_thread_count)

    def add_job(self, func):
        self._debug("Adding job %s" % str(func)) 
        self.queue.append(func)
        self.job_semaphore.release()

    def take_job(self):
        """
        Called by worker threads to take a job from the queue.
        Will block until a job is available. Don't call this
        from outside of a WorkerThread instance that belongs to this
        pool.
        """
        self.job_semaphore.acquire()

        # Do we have extraneous workers to dispose of?
        if self.desired_thread_count < self.actual_thread_count:
            with self.resize_lock:
                # Double-check now that we're holding the lock
                if self.desired_thread_count < self.actual_thread_count:
                    # Return None to tell the worker thread to exit
                    return None

        return self.queue.popleft()


class WorkerThread(Thread):
    pool = None

    def __init__(self, pool):
        self.pool = pool
        Thread.__init__(self, group=None)

    def _debug(self, message):
        with (self.pool.debug_print_lock):
            #print self.name+": "+message
            pass

    def run(self):
        self._debug("Starting up")
        while (1):

            self._debug("Waiting for a job")
            job_func = self.pool.take_job()

            if job_func is None:
                self._debug("Pool has asked me to terminate. Bye!")
                return

            self._debug("Running job "+str(job_func))
            job_func()
            self._debug("Finished running job "+str(job_func))



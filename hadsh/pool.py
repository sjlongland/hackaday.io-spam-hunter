from tornado.gen import coroutine, Future, Return
from tornado.ioloop import IOLoop
from tornado.queues import Queue
from tornado.locks import Semaphore
from threading import Thread
from multiprocessing import cpu_count
from sys import exc_info


class WorkerPool(object):
    """
    The WorkerPool object represents a pool of worker threads which
    each run a task in an external thread.
    """
    def __init__(self, workers=None, io_loop=None):
        if workers is None:
            workers = cpu_count()
        if io_loop is None:
            io_loop = IOLoop.current()
        self._io_loop = io_loop
        self._sem = Semaphore(workers)
        self._queue = Queue()
        self._active = False

    @coroutine
    def apply(self, func, args=None, kwds=None):
        """
        Enqueue a request to be processed in a worker thread.
        """
        if args is None: args = ()
        if kwds is None: kwds = {}

        # Our result placeholder
        future = Future()

        # Enqueue the request
        yield self._queue.put((future, func, args, kwds))

        # Kick-start the queue manager if not already running
        self._io_loop.add_callback(self._queue_manager)

        # Get back the result
        result = yield future
        raise Return(result)

    @coroutine
    def _apply(self, future, func, args=None, kwds=None):
        """
        Execute a function in a worker thread.  Wrapper function.
        """
        yield self._sem.acquire()

        # Receive the result back; sets the future result
        def _recv_result(err, res):
            self._sem.release()
            if err is not None:
                future.set_exc_info(err)
            else:
                future.set_result(res)

        # Run the function; in a worker thread
        def _exec():
            err = None
            res = None

            try:
                res = func(*args, **kwds)
            except:
                err = exc_info()

            self._io_loop.add_callback(_recv_result, err, res)

        # Spawn the worker thread
        thread = Thread(target=_exec)
        thread.start()

    @coroutine
    def _queue_manager(self):
        """
        Queue manager co-routine.
        """
        if self._active:
            # Already active
            return

        try:
            self._active = True
            while True:
               (future, func, args, kwds) = yield self._queue.get()
               self._io_loop.add_callback(
                       self._apply, future, func, args, kwds)
        finally:
            self._active = False

import threading

from tornado.gen import coroutine, Future, Return
from tornado.ioloop import IOLoop
import imagehash
from PIL import Image
from sys import exc_info
from io import BytesIO


class ImageHasher(object):
    def __init__(self, log, pool, io_loop=None):
        if io_loop is None:
            io_loop = IOLoop.current()
        self._log = log
        self._io_loop = io_loop
        self._pool = pool

    @coroutine
    def hash(self, avatar, algorithm):
        log = self._log.getChild('avatar[%d]' % avatar.avatar_id)
        future = Future()

        if not hasattr(imagehash, algorithm):
            raise ValueError('unknown algorithm %s' % algorithm)

        # Handing the value back to the coroutine
        def _on_done(result):
            if isinstance(result, tuple):
                log.audit('Passing back exception')
                future.set_exc_info(result)
            else:
                log.audit('Passing back result')
                future.set_result(result)

        # What to do in the thread pool
        def _do_hash(image_data, algorithm):
            try:
                log.audit('Opening image')
                image = Image.open(BytesIO(image_data))

                algofunc = getattr(imagehash, algorithm)
                res = algofunc(image)

                self._io_loop.add_callback(_on_done, res.hash)
            except:
                log.exception('Failed to hash')
                self._io_loop.add_callback(_on_done, exc_info())

        # Run the above in the thread pool:
        yield self._pool.apply(_do_hash, (avatar.avatar, algorithm))

        # Wait for the result
        hash_data = yield future

        # Return the data
        raise Return(hash_data)

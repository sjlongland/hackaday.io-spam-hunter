from psycopg2 import connect
from tornado.ioloop import IOLoop
from tornado.gen import coroutine, Return
from concurrent.futures import Future
import threading

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


class Database(object):
    def __init__(self, db_uri, **kwargs):
        """
        Parse the database URI and keyword arguments.
        """
        parsed_uri = urlparse(db_uri)

        if '@' in parsed_uri.netloc:
            (user_password, host_port) = \
                    parsed_uri.netloc.split('@', 1)
        else:
            user_password = None
            host_port = parsed_uri.netloc

        if user_password and (':' in user_password):
            (user, password) = user_password.split(':', 1)
        else:
            user = user_password or None
            password = None

        if host_port:
            # Beware of IPv6 literals
            try:
                end_literal = host_port.index(']')
            except ValueError:
                end_literal = None

            if end_literal:
                assert host_port[0] == '['
                host = host_port[0:end_literal+1]

                port = host_port[end_literal+1:]
                if port.startswith(':'):
                    port = int(port[1:])
                else:
                    port = None
            else:
                # IPv4 or hostname
                if ':' in host_port:
                    (host, port) = host_port.split(':', 1)
                    port = int(port)
                else:
                    host = host_port
                    port = None
        else:
            host = None
            port = None

        self._db_args = dict(
            dbname=parsed_uri.path[1:],
            user=user, password=password,
            host=host, port=port, **kwargs)

        self._conn_ioloop = None
        self._conn_thread = None
        self._conn = None


    @coroutine
    def connect(self):
        """
        Connect to the server
        """
        assert self._conn is None
        assert self._conn_ioloop is None

        future = Future()
        io_loop = IOLoop()
        thread = threading.Thread(
                target=io_loop.start,
                name='DatabaseThread')
        thread.start()

        def _connect():
            try:
                conn = connect(**self._db_args)
                self._conn = conn
                self._conn_ioloop = io_loop
                self._conn_thread = thread
                future.set_result(None)
            except Exception as ex:
                io_loop.stop()
                future.set_exception(ex)
        io_loop.add_callback(_connect)
        yield future


    def close(self):
        if self._conn is None:
            return

        def _close():
            self._conn.close()
            self._conn_ioloop.stop()
        self._conn_ioloop.add_callback(_close)
        self._conn_thread.join()

        self._conn = None
        self._conn_ioloop = None
        self._conn_thread = None


    @coroutine
    def query(self, sql, *args, commit=False):
        if self._conn is None:
            yield self.connect()

        assert self._conn is not None
        assert self._conn_ioloop is not None

        future = Future()
        def _query():
            try:
                with self._conn:
                    with self._conn.cursor() as cur:
                        cur = self._conn.cursor()
                        cur.execute(sql, args)

                        if cur.description:
                            res = cur.fetchall()
                        else:
                            res = None

                        if commit:
                            self._conn.commit()

                        future.set_result(res)
            except Exception as ex:
                future.set_exception(ex)
        self._conn_ioloop.add_callback(_query)

        result = yield future
        raise Return(result)

from tornado.gen import coroutine

from .trait import *
from .avatar import avatar_init
from .spamname import spamname_init
from .aboutmelinktrait import aboutmelink_init

@coroutine
def init_traits(app, log):
    yield avatar_init(app, log)
    yield spamname_init(app, log)
    yield aboutmelink_init(app, log)

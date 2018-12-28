from .trait import *
from .avatar import avatar_init
from .spamname import spamname_init
from .aboutmelinktrait import aboutmelink_init

def init_traits(app, log):
    avatar_init(app, log)
    spamname_init(app, log)
    aboutmelink_init(app, log)

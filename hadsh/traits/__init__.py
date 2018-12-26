from .trait import *
from .avatar import avatar_init
from .spamname import spamname_init
from .aboutmelinktrait import aboutmelink_init

def init_traits(db, log):
    avatar_init(db, log)
    spamname_init(db, log)
    aboutmelink_init(db, log)

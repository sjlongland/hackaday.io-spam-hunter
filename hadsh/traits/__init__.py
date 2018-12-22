from .trait import *
from .avatar import avatar_init
from .spamname import spamname_init

def init_traits(db):
    avatar_init(db)
    spamname_init(db)

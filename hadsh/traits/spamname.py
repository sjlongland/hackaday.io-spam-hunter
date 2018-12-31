from tornado.gen import coroutine, Return
from .trait import SingletonTrait, SingletonTraitInstance, \
        UserSingletonTraitInstance
import re

NAME_PATTERNS = list(map(lambda p : re.compile(p),
    [
        # Could also be a radio call-sign, but most probably that Polish
        # twerp.  Manually verify.
        r'^[a-zA-Z][0-9][a-zA-Z][a-zA-Z][a-zA-Z]$',
        # Definite spammer display name.
        r'[0-9][a-zA-Z][0-9][0-9][0-9][a-zA-Z]$',
    ]))


class SpammyNameTrait(SingletonTrait):
    _TRAIT_CLASS = "spamname"

    @coroutine
    def _assess(self, user, log):
        for pattern in NAME_PATTERNS:
            if pattern.search(user.screen_name):
                raise Return(UserSingletonTraitInstance(
                        user,
                        SingletonTraitInstance(self),
                        1))


# Instantiate these instances and register them.
@coroutine
def spamname_init(app, log):
    yield SpammyNameTrait.init(app, log)

from .trait import SingletonTrait, SingletonTraitInstance, \
        UserSingletonTraitInstance
from ..db import model
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

    def _assess(self, user, log):
        for pattern in NAME_PATTERNS:
            if pattern.search(user.screen_name):
                return UserSingletonTraitInstance(
                        user,
                        SingletonTraitInstance(self),
                        1)


# Instantiate these instances and register them.
def spamname_init(db):
    assert SpammyNameTrait(db)

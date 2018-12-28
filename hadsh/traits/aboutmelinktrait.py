from .trait import SingletonTrait, SingletonTraitInstance, \
        UserSingletonTraitInstance
from ..db import model
import re


class AboutMeLinkTrait(SingletonTrait):
    _TRAIT_CLASS = "aboutmelink"

    def _assess(self, user, log):
        for link in user.links:
            if link.title == user.detail.about_me:
                return UserSingletonTraitInstance(
                        user,
                        SingletonTraitInstance(self),
                        1)


# Instantiate these instances and register them.
def aboutmelink_init(app, log):
    assert AboutMeLinkTrait(app, log)

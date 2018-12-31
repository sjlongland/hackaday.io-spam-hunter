from tornado.gen import coroutine, Return

from .trait import SingletonTrait, SingletonTraitInstance, \
        UserSingletonTraitInstance
from ..db import model
import re


class AboutMeLinkTrait(SingletonTrait):
    _TRAIT_CLASS = "aboutmelink"

    @coroutine
    def _assess(self, user, log):
        user_links = yield model.UserLink.fetch(
                self._db, 'user_id=%s', user.user_id)
        user_detail = yield user.get_detail()
        for link in user_links:
            if link.title == user_detail.about_me:
                raise Return(UserSingletonTraitInstance(
                        user,
                        SingletonTraitInstance(self),
                        1))


# Instantiate these instances and register them.
@coroutine
def aboutmelink_init(app, log):
    yield AboutMeLinkTrait.init(app, log)

from tornado.gen import coroutine, Return

from .trait import Trait, TraitType, TraitInstance, UserTraitInstance
from ..db import model


class BaseAvatarTrait(Trait):
    _TRAIT_TYPE = TraitType.AVATAR_HASH

    @coroutine
    def _get_trait_instance(self, avatar_hash):
        # Create the instance if not already there.
        yield self._db.query('''
            INSERT INTO "trait_instance"
                (trait_id, trait_hash_id, score, count)
            VALUES
                (%s, %s, 0, 0)
            ON CONFLICT DO NOTHING
        ''', self.trait_id, avatar_hash.hash_id, commit=True)

        trait_instance = yield model.TraitInstanceAvatarHash.fetch(
                self._db, 'trait_id=%s AND trait_hash_id=%s',
                self.trait_id, avatar_hash.hash_id,
                single=True
        )
        raise Return(TraitInstance(self, trait_instance))

    @coroutine
    def _assess(self, user, log):
        avatar_hash = yield model.AvatarHash.fetch(
                self._db,
                '''
                hashalgo=%s
                AND
                hash_id IN (
                    SELECT
                        hash_id
                    FROM
                        "avatar_hash_assoc"
                    WHERE
                        avatar_id=%s
                    )
                LIMIT 1
                ''', self._HASH_ALGO, user.avatar_id,
                single=True)
        log.audit('User %s avatar %s hash algo %s, hash %s',
                user, user.avatar_id, self._HASH_ALGO, avatar_hash)
        if avatar_hash is not None:
            trait_inst = yield self._get_trait_instance(avatar_hash)
            raise Return(UserTraitInstance(
                    user, trait_inst, 1))


class SHA512AvatarTrait(BaseAvatarTrait):
    _HASH_ALGO = 'sha512'
    _TRAIT_CLASS = 'avatar.sha512'


class AverageHashAvatarTrait(BaseAvatarTrait):
    _HASH_ALGO = 'avghash'
    _TRAIT_CLASS = 'avatar.avghash'


class PHashAvatarTrait(BaseAvatarTrait):
    _HASH_ALGO = 'phash'
    _TRAIT_CLASS = 'avatar.phash'


class DHashAvatarTrait(BaseAvatarTrait):
    _HASH_ALGO = 'dhash'
    _TRAIT_CLASS = 'avatar.dhash'


class WHashAvatarTrait(BaseAvatarTrait):
    _HASH_ALGO = 'whash'
    _TRAIT_CLASS = 'avatar.whash'


# Instantiate these instances and register them.
@coroutine
def avatar_init(app, log):
    yield SHA512AvatarTrait.init(app, log)
    yield AverageHashAvatarTrait.init(app, log)
    yield PHashAvatarTrait.init(app, log)
    yield DHashAvatarTrait.init(app, log)
    yield WHashAvatarTrait.init(app, log)

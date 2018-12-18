from .trait import Trait, TraitType, TraitInstance, UserTraitInstance
from ..db import model


class BaseAvatarTrait(Trait):
    _TRAIT_TYPE = TraitType.AVATAR_HASH

    def _get_trait_instance(self, avatar_hash):
        trait_instance = self._db.query(model.TraitInstanceAvatarHash).filter(
                model.TraitInstance.trait_id == self._trait.trait_id,
                model.TraitInstanceAvatarHash.trait_hash_id ==
                    avatar_hash.hash_id
        ).one_or_none()
        if trait_instance is None:
            trait_instance = model.TraitInstanceAvatarHash(
                    trait_id=self._trait.trait_id,
                    trait_hash_id=avatar_hash.hash_id,
                    score=0, count=0)
            self._db.add(trait_instance)
            self._db.commit()
        return TraitInstance(self, trait_instance)

    def _assess(self, user, log):
        for ah in user.avatar.hashes:
            if ah.hashalgo == self._HASH_ALGO:
                return UserTraitInstance(
                        user, self._get_trait_instance(ah), 1)


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
def avatar_init(db):
    assert SHA512AvatarTrait(db)
    assert AverageHashAvatarTrait(db)
    assert PHashAvatarTrait(db)
    assert DHashAvatarTrait(db)
    assert WHashAvatarTrait(db)

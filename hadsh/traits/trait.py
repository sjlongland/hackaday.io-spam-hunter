#!/usr/bin/env python

from ..db import model
from enum import Enum


class TraitType(Enum):
    """
    Trait types
    """
    SINGLETON   = 'singleton'
    AVATAR_HASH = 'avatar_hash'
    STRING      = 'string'


class Trait(object):
    """
    A base class for all Traits
    """

    # All known traits
    _ALL_TRAITS = {}

    def __init__(self, db):
        # This is a singleton class
        assert not hasattr(self.__class__, '_instance')
        assert self._TRAIT_CLASS not in self._ALL_TRAITS

        trait = db.query(model.Trait).filter(
                model.Trait.trait_class == self._TRAIT_CLASS).one_or_none()

        if trait is None:
            trait = model.Trait(
                    trait_class=self._TRAIT_CLASS,
                    trait_type=self._TRAIT_TYPE.value,
                    score=0, count=0)
            db.add(trait)
            db.commit()

        self._db = db
        self._trait = trait
        self._ALL_TRAITS[self._TRAIT_CLASS] = self

    @property
    def trait_id(self):
        return self._trait.trait_id

    @property
    def weight(self):
        return self._trait.weight

    @classmethod
    def assess(cls, user, log):
        """
        Assess the given user for their traits.
        """
        user_traits = []

        for trait in list(cls._ALL_TRAITS.values()):
            trait_log = log.getChild(trait._TRAIT_CLASS)
            try:
                user_trait = trait._assess(user, trait_log)
            except:
                trait_log.exception('Failed to assess %s for trait %s',
                        user, trait)
                continue

            if user_trait is not None:
                user_traits.append(user_trait)
        return user_traits

    def _assess(self, user, log):
        """
        Assess the user against this particular trait.
        """
        raise NotImplementedError()


class BaseTraitInstance(object):
    """
    An instance of a given trait, linked to a user.
    """
    def __init__(self, trait):
        self._trait = trait

    @property
    def score(self):
        raise NotImplementedError()

    @property
    def count(self):
        raise NotImplementedError()

    @property
    def trait(self):
        return self._trait

    @property
    def instance(self):
        raise NotImplementedError()

    def increment(self, count, direction):
        """
        Increment the score and count.
        """
        raise NotImplementedError()

    @property
    def _db(self):
        return self._trait._db


class BaseUserTraitInstance(object):
    """
    An instance of a trait linked to a user.
    """
    def __init__(self, user, trait_instance, count):
        self._user = user
        self._trait_instance = trait_instance
        self._count = count
        self._user_trait_obj = None

    @property
    def _user_trait(self):
        return NotImplementedError()

    @property
    def count(self):
        return self._count

    @property
    def weighted_score(self):
        if self.trait_count == 0:
            return 0.0

        return (float(self.trait_score) * self.trait.weight) \
                / float(self.trait_count)

    @property
    def trait_score(self):
        return self._trait_instance.score

    @property
    def trait_count(self):
        return self._trait_instance.count

    @property
    def trait(self):
        return self._trait_instance.trait

    @property
    def instance(self):
        return self._trait_instance.instance

    def discard(self):
        """
        Remove a link between a trait and a user from the database.
        """
        if self._user_trait is not None:
            self._user_trait.delete()
        self._db.commit()

    def persist(self):
        """
        Persist this user trait instance count in the database.
        """
        raise NotImplementedError()

    def increment_trait(self, direction):
        """
        Increment the trait instance score
        """
        self._trait_instance.increment(self.count, direction)

    @property
    def _db(self):
        return self._trait_instance._db


class TraitInstance(BaseTraitInstance):
    """
    An instance of a given trait.
    """
    def __init__(self, trait, instance):
        super(TraitInstance, self).__init__(trait)
        self._instance = instance

    @property
    def trait_inst_id(self):
        return self._instance.trait_inst_id

    @property
    def score(self):
        return self._instance.score

    @property
    def count(self):
        return self._instance.count

    @property
    def instance(self):
        return self._instance.instance

    def increment(self, count, direction):
        """
        Increment the score and count.
        """
        if count == 0:
            return

        self._instance.score += (count * direction)
        self._instance.count += count
        self._db.commit()


class UserTraitInstance(BaseUserTraitInstance):
    """
    An instance of a trait linked to a user.
    """
    def __init__(self, user, trait_instance, count):
        super(UserTraitInstance, self).__init__(user, trait_instance, count)
        self._user_trait_obj = None

    @property
    def _user_trait(self):
        if self._user_trait_obj is None:
            self._user_trait_obj = self._db.query( \
                    model.UserTraitInstance \
            ).filter(
                    model.UserTraitInstance.user_id == self._user.user_id,
                    model.UserTraitInstance.trait_inst_id \
                        == self._trait_instance.trait_inst_id
            ).one_or_none()

        return self._user_trait_obj

    def persist(self):
        """
        Persist this user trait instance count in the database.
        """
        if self._user_trait is None:
            # No existing instance, create it.
            self._user_trait_obj = model.UserTraitInstance(
                    user_id=self._user.user_id,
                    trait_inst_id=self._trait_instance.trait_inst_id,
                    count=self.count)
            self._db.add(self._user_trait_obj)
        else:
            # Existing instance, update if not matching.
            if self._user_instance.count == self.count:
                return

            self._user_instance.count = self.count
        self._db.commit()


class SingletonTrait(Trait):
    """
    A trait which is a singleton.
    """
    _TRAIT_TYPE = TraitType.SINGLETON

    @property
    def score(self):
        return self._trait.score

    @property
    def count(self):
        return self._trait.count

    def increment(self, count, direction):
        """
        Increment the score and count.
        """
        if count == 0:
            return

        self._trait.score += (count * direction)
        self._trait.count += count
        self._db.commit()


class SingletonTraitInstance(BaseTraitInstance):
    """
    An instance of a given singleton trait.
    """
    def __init__(self, trait):
        super(SingletonTraitInstance, self).__init__(trait)

    @property
    def score(self):
        return self._trait.score

    @property
    def count(self):
        return self._trait.count

    @property
    def instance(self):
        return None

    def increment(self, count, direction):
        """
        Increment the score and count.
        """
        self._trait.increment(count, direction)


class UserSingletonTraitInstance(BaseUserTraitInstance):
    """
    A singleton trait linked to a user.
    """
    def __init__(self, user, trait_instance, count):
        super(UserSingletonTraitInstance, self).__init__(\
                user, trait_instance, count)
        self._user_trait_obj = None

    @property
    def _user_trait(self):
        if self._user_trait_obj is None:
            self._user_trait_obj = self._db.query(model.UserTrait).filter(
                    model.UserTrait.user_id == self._user.user_id,
                    model.UserTrait.trait_id == \
                        self._trait_instance.trait.trait_id
            ).one_or_none()
        return self._user_trait_obj

    def persist(self):
        """
        Persist this user trait instance count in the database.
        """
        if self._user_trait is None:
            # No existing instance, create it.
            self._user_trait_obj = model.UserTrait(
                    user_id=self._user.user_id,
                    trait_id=self._trait_instance.trait.trait_id,
                    count=self.count)
            self._db.add(self._user_trait_obj)
        else:
            # Existing instance, update if not matching.
            if self._user_trait.count == self.count:
                return

            self._user_trait.count = self.count
        self._db.commit()

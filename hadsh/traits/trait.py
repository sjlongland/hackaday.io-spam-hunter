#!/usr/bin/env python

from tornado.gen import coroutine, Return

from ..db import model
from enum import Enum


class TraitType(Enum):
    """
    Trait types
    """
    SINGLETON   = 'singleton'
    AVATAR_HASH = 'avatar_hash'
    STRING      = 'string'
    PAIR        = 'pair'


class DatabaseShutdown(object):
    """
    Lives in the "thread local" storage and tells us when
    that gets 'freed' so that we can close the database connection.
    """
    def __init__(self, db):
        self._db = db

    def __del__(self):
        self._db.close()


class Trait(object):
    """
    A base class for all Traits
    """

    # All known traits
    _ALL_TRAITS = {}

    def __init__(self, trait, app, log):
        # This is a singleton class
        assert not hasattr(self.__class__, '_instance')
        assert self._TRAIT_CLASS not in self._ALL_TRAITS

        self._trait = trait
        self._log = log.getChild(self._TRAIT_CLASS)
        self._app = app
        self._db = app._db
        self._ALL_TRAITS[self._TRAIT_CLASS] = self

    @classmethod
    @coroutine
    def init(cls, app, log):
        db = app._db
        # Create the trait if it doesn't already exist
        yield db.query('''
            INSERT INTO "trait"
                (trait_class, trait_type, score, count, weight)
            VALUES
                (%s, %s, 0, 0, 1.0)
            ON CONFLICT DO NOTHING''',
            cls._TRAIT_CLASS,
            cls._TRAIT_TYPE.value,
            commit=True)

        # Fetch the trait instance
        traits = yield model.Trait.fetch(
                db, 'trait_class=%s', cls._TRAIT_CLASS)
        assert cls(traits[0], app, log)

    @property
    def trait_id(self):
        return self._trait.trait_id

    @property
    def weight(self):
        return self._trait.weight

    @classmethod
    @coroutine
    def assess(cls, user, log):
        """
        Assess the given user for their traits.
        """
        assert isinstance(user, model.User)
        user_traits = []

        for trait in list(cls._ALL_TRAITS.values()):
            trait_log = log.getChild(trait._TRAIT_CLASS)
            trait_log.audit('Assessing %s', user)
            try:
                user_trait = yield trait._assess(
                        user, trait_log)
            except:
                trait_log.exception('Failed to assess %s for trait %s',
                        user, trait)
                continue

            if user_trait is not None:
                user_traits.append(user_trait)
        log.audit('User %s has %s', user, user_traits)
        raise Return(user_traits)

    @coroutine
    def _assess(self, user, log):
        """
        Assess the user against this particular trait.
        """
        raise NotImplementedError()


class StringTrait(Trait):
    """
    Helper sub-class for string-based traits.
    """
    _TRAIT_TYPE = TraitType.STRING

    @coroutine
    def _get_trait_instance(self, value):
        # Create the instance if not already there.
        yield self._db.query('''
            INSERT INTO "trait_instance"
                (trait_id, trait_string, score, count)
            VALUES
                (%s, %s, 0, 0)
            ON CONFLICT DO NOTHING
        ''', self.trait_id, value, commit=True)

        trait_instances = yield model.TraitInstanceString.fetch(
                self._db, 'trait_id=%s AND trait_string=%s',
                self.trait_id, value
        )
        raise Return(TraitInstance(self, trait_instances[0]))


class BaseTraitInstance(object):
    """
    An instance of a given trait, linked to a user.
    """
    def __init__(self, trait):
        assert isinstance(trait, Trait)
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
        assert isinstance(trait_instance, BaseTraitInstance)
        self._user_id = user.user_id
        self._trait_instance = trait_instance
        self._count = count
        self._user_trait_id = None

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
        self._close_db()

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
        self._log = trait._log.getChild('inst[%s]' % instance.instance)

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
        self._log.debug('Adjust instance score=%d count=%d',
                self._instance.score, self._instance.count)
        self._close_db()


class UserTraitInstance(BaseUserTraitInstance):
    """
    An instance of a trait linked to a user.
    """
    @property
    def _user_trait(self):
        try:
            return self._local.user_trait
        except AttributeError:
            pass

        user_trait = self._db.query( \
                model.UserTraitInstance \
        ).filter(
                model.UserTraitInstance.user_id == self._user_id,
                model.UserTraitInstance.trait_inst_id \
                    == self._trait_instance.trait_inst_id
        ).one_or_none()

        if user_trait is not None:
            self._local.user_trait = user_trait
        return user_trait

    def persist(self):
        """
        Persist this user trait instance count in the database.
        """
        if self._user_trait is None:
            # No existing instance, create it.
            self._local.user_trait = model.UserTraitInstance(
                    user_id=self._user_id,
                    trait_inst_id=self._trait_instance.trait_inst_id,
                    count=self.count)
            self._db.add(self._local.user_trait)
        else:
            # Existing instance, update if not matching.
            if self._user_instance.count == self.count:
                return

            self._user_instance.count = self.count
        self._db.commit()
        self._close_db()


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
        self._log.debug('Adjust trait score=%d count=%d',
                self._trait.score, self._trait.count)
        self._close_db()


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
    @property
    def _user_trait(self):
        try:
            return self._local.user_trait
        except AttributeError:
            pass

        user_trait = self._db.query(model.UserTrait).filter(
                model.UserTrait.user_id == self._user_id,
                model.UserTrait.trait_id == \
                    self._trait_instance.trait.trait_id
        ).one_or_none()

        if user_trait is not None:
            self._local.user_trait = user_trait

        return user_trait

    def persist(self):
        """
        Persist this user trait instance count in the database.
        """
        if self._user_trait is None:
            # No existing instance, create it.
            self._local.user_trait = model.UserTrait(
                    user_id=self._user_id,
                    trait_id=self._trait_instance.trait.trait_id,
                    count=self.count)
            self._db.add(self._local.user_trait)
        else:
            # Existing instance, update if not matching.
            if self._user_trait.count == self.count:
                return

            self._user_trait.count = self.count
        self._db.commit()
        self._close_db()

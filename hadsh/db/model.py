from functools import partial
from tornado.gen import coroutine, Return

import base64


class Row(object):
    """
    Raw row class, simplifies manipulation of tables.
    """
    def __init__(self, db, row):
        for column in self._COLUMNS_:
            if not hasattr(self, column):
                setattr(self.__class__, column,
                        property(\
                            partial(\
                                lambda c, s : s._get_col(c), column),
                            partial(\
                                lambda c, s, val : s._set_col(c, val), column),
                            partial(\
                                lambda c, s : s._del_col(c), column)))
        self._data = dict(zip(self._COLUMNS_, row))
        self._dirty = {}
        self._db = db


    @classmethod
    @coroutine
    def fetch(cls, db, where, *args):
        rows = yield db.query(
                '''
                    SELECT
                        %(columns)s
                    FROM
                        "%(table)s"
                    WHERE
                        %(where)s
                ''' % {
                    'columns': cls._COLUMNS_.join(', '),
                    'table': cls._TABLE_,
                    'where': where
                }, *args)
        raise Return([
            cls(db, row) for row in rows
        ])


    @coroutine
    def commit(self):
        try:
            dirty_data      = self._dirty.copy()
            self._dirty     = {}
            dirty_columns   = list(dirty_data.keys())
            dirty_args      = [dirty_data[c] for c in dirty_columns]
            where_args      = [self._data[c] for c in self._PRIMARY_KEYS_]

            sql = '''UPDATE
                "%(table)s"
            SET
                %(updates)s
            WHERE
                %(criteria)s''' % {
                        'table': self._TABLE_,
                        'updates': ', '.join([
                            '"%s"=%%s' % (c,)
                            for c in dirty_columns
                        ]),
                        'criteria': ' AND '.join([
                            '("%s"=%%s)' % (c,)
                            for c in self._PRIMARY_KEYS_
                        ])
                }

            yield self._db.query(sql,
                    *tuple(dirty_args + where_args),
                    commit=True)
            self._data.update(dirty_data)
        except:
            dirty_data.update(self._dirty)
            self._dirty = dirty_data
            raise

    def _get_col(self, column):
        return self._dirty.get(column, self._data[column])

    def _set_col(self, column, value):
        self._dirty[column] = value

    def _del_col(self, column, value):
        self._dirty.pop(column, None)


class User(Row):
    """
    All recognised Hackaday.io users, including legitmate ones.
    """

    # Table
    _TABLE_         = 'user'

    # Primary key
    _PRIMARY_KEYS_  = ['user_id']

    # Columns
    _COLUMNS_       = [
            'user_id',
            'screen_name',
            'url',
            'avatar_id',
            'created',
            'had_created',
            'last_update'
    ]

    def __repr__(self):
        return 'User(user_id=%r, screen_name=%r)' \
                % (self.user_id, self.screen_name)

    @coroutine
    def get_groups(self):
        """
        Return a set of groups linked to this user.
        """
        groups = yield self._db.query(
                '''
                SELECT
                    g.name
                FROM
                    "group" g,
                    "user_group_assoc" uga
                WHERE
                    g.group_id=uga.group_id
                AND
                    uga.user_id=%s
        ''', self.user_id)
        raise Return(set([
            name for (name,) in groups
        ]))

    @coroutine
    def set_groups(self, groups):
        """
        Set the list of groups linked to this user.
        """
        yield self.add_groups(groups)
        yield self.mask_groups(groups)

    @coroutine
    def add_groups(self, groups):
        # Add the new groups
        yield self._db.query('''
            INSERT INTO "user_group_assoc"
                (user_id, group_id)
            SELECT
                %s, group_id
            FROM
                "group"
            WHERE
                name IN %s
            ON CONFLICT DO NOTHING
        ''', self.user_id, tuple(groups))

    @coroutine
    def mask_groups(self, groups):
        # Remove groups not listed
        yield self._db.query('''
            DELETE FROM "user_group_assoc"
            WHERE
                user_id=%S
            AND
                group_id NOT IN (
                    SELECT
                        %s, group_id
                    FROM
                        "group"
                    WHERE
                        name IN %s
                )
        ''', self.user_id, tuple(groups))

    @coroutine
    def get_detail(self):
        details = yield UserDetail.fetch(self._db,
                'user_id=%s LIMIT 1', self.user_id)
        if len(details) == 1:
            raise Return(details[0])
        else:
            raise Return(None)

    @coroutine
    def get_deferral(self):
        deferrals = yield DeferredUser.fetch(self._db,
                'user_id=%s LIMIT 1', self.user_id)
        if len(deferrals) == 1:
            raise Return(deferrals[0])
        else:
            raise Return(None)


class Account(Row):
    _TABLE_         = 'account'
    _PRIMARY_KEYS_  = ['user_id']
    _COLUMNS_       = [
            'user_id',
            'name',
            'hashedpassword',
            'changenextlogin'
    ]

    @coroutine
    def get_user(self):
        users = yield User.fetch(self._db,
                'user_id=%s LIMIT 1', self.user_id)
        if len(users) == 1:
            raise Return(users[0])
        else:
            raise Return(None)


class DeferredUser(Row):
    """
    Object to represent when a user account has been added but inspection
    has been deferred.
    """

    _TABLE_         = 'deferred_user'
    _PRIMARY_KEYS_  = ['user_id']
    _COLUMNS_       = [
            'user_id',
            'inspect_time',
            'inspections'
    ]


class Group(Row):
    """
    Groups used for classifying users.
    """

    _TABLE_         = 'group'
    _PRIMARY_KEYS_  = ['group_id']
    _COLUMNS_       = [
            'group_id',
            'name'
    ]

    def __repr__(self):
        return 'Group(group_id=%r, name=%r)' \
                % (self.group_id, self.name)


class Session(Row):
    """
    Session token storage.  The session ID will be emitted to the user, so
    we use a UUID field to make the value unguessable.
    """
    _TABLE_         = 'session'
    _PRIMARY_KEYS_  = ['session_id']
    _COLUMNS_       = [
        'session_id',
        'user_id',
        'expiry_date'
    ]

    def __repr__(self):
        return 'Session(user=%r)' \
                % (self.user)

    @coroutine
    def get_user(self):
        users = yield User.fetch(self._db,
                'user_id=%s LIMIT 1', self.user_id)
        if len(users) == 1:
            raise Return(users[0])
        else:
            raise Return(None)


class UserDetail(Row):
    """
    Detail on 'suspect' users.  Only users that have been identified as
    possibly spammy by the search algorithm, or users that have been flagged
    as spammy by logged-in users, appear here.
    """
    _TABLE_         = 'user_detail'
    _PRIMARY_KEYS_  = ['user_id']
    _COLUMNS_       = [
        'user_id',
        'about_me',
        'who_am_i',
        'what_i_would_like_to_do',
        'location',
        'projects'
    ]


class UserLink(Row):
    """
    Links attached to 'suspect' users.
    """
    _TABLE_         = 'user_link'
    _PRIMARY_KEYS_  = ['user_id', 'url']
    _COLUMNS_       = [
            'user_id',
            'title',
            'url'
    ]

    def __repr__(self):
        return 'UserLink(user_id=%r, title=%r, url=%r)' \
                % (self.user_id, self.title, self.url)


class Avatar(Row):
    """
    A cache of users' avatars, as some share the same image.
    """
    _TABLE_         = 'avatar'
    _PRIMARY_KEYS_  = ['avatar_id']
    _COLUMNS_       = [
            'avatar_id',
            'url',
            'avatar',
            'avatar_type'
    ]

    @coroutine
    def get_hashes(self):
        hashes = yield AvatarHash.fetch(self._db,
                '''
                hash_id IN (
                    SELECT
                        hash_id
                    FROM
                        "avatar_hash_assoc"
                    WHERE
                        avatar_id=%s
                )''', self.avatar_id)
        raise Return(dict([
            (h.hashalgo, h) for h in hashes
        ]))


class AvatarHash(Row):
    """
    A hash of users' avatars and their scores.
    """
    _TABLE_         = 'avatar_hash'
    _PRIMARY_KEYS_  = ['hash_id']
    _COLUMNS_       = [
            'hash_id',
            'hashalgo',
            'hashdata'
    ]

    @property
    def hashstr(self):
        return base64.a85encode(self.hashdata)

    def __repr__(self):
        return '<%s #%d %s %s>' % (
                self.__class__.__name__,
                self.hash_id,
                self.hashalgo,
                self.hashstr)


class Tag(Row):
    """
    A list of tags seen applied to users' accounts.
    """
    _TABLE_         = 'tag'
    _PRIMARY_KEYS_  = ['tag_id']
    _COLUMNS_       = [
            'tag_id',
            'tag'
    ]


class NewestUserPageRefresh(Row):
    """
    A record of when each page of the "newst users" list was last refreshed.
    """
    _TABLE_         = 'newest_user_page_refresh'
    _PRIMARY_KEYS_  = ['page_num']
    _COLUMNS_       = [
            'page_num',
            'refresh_date'
    ]


class NewUser(Row):
    """
    A record of a new user that is to be inspected.
    """
    _TABLE_         = 'new_user'
    _PRIMARY_KEYS_  = ['user_id']
    _COLUMNS_       = [
            'user_id'
    ]


class Hostname(Row):
    """
    A hostname that appears in the links of user profiles.
    """
    _TABLE_         = 'hostname'
    _PRIMARY_KEYS_  = ['hostname_id']
    _COLUMNS_       = [
            'hostname_id',
            'hostname',
            'score',
            'count'
    ]

    def __repr__(self):
        return 'Hostname(hostname_id=%r, hostname=%r, score=%r, count=%r)' \
                % (self.hostname_id, self.hostname, self.score, self.count)


class Word(Row):
    """
    A single word in the vocabulary of the Hackaday.io community.
    """
    _TABLE_         = 'word'
    _PRIMARY_KEYS_  = ['word_id']
    _COLUMNS_       = [
            'word_id',
            'word',
            'score',
            'count'
    ]

    def __repr__(self):
        return 'Word(word_id=%r, word=%r, score=%r, count=%r)' \
                % (self.word_id, self.word, self.score, self.count)


class WordAdjacent(Row):
    """
    How often two words appear next to each other.
    """
    _TABLE_         = 'word_adjacent'
    _PRIMARY_KEYS_  = ['proceeding_id', 'following_id']
    _COLUMNS_       = [
            'proceeding_id',
            'following_id',
            'score',
            'count'
    ]

    def __repr__(self):
        return 'WordAdjacent(proceeding=%r, following=%r, score=%r, count=%r)' \
                % (self.proceeding_id, self.following_id, self.score, self.count)


class UserWord(Row):
    """
    Words used by a given user
    """
    _TABLE_         = 'user_word'
    _PRIMARY_KEYS_  = ['user_id', 'word_id']
    _COLUMNS_       = [
            'user_id',
            'word_id',
            'count'
    ]

    def __repr__(self):
        return 'UserWord(user=%r, word=%r, count=%r)' \
                % (self.user_id, self.word_id, self.count)


class UserHostname(Row):
    """
    Hostnames used by a given user
    """
    _TABLE_         = 'user_hostname'
    _PRIMARY_KEYS_  = ['user_id', 'hostname_id']
    _COLUMNS_       = [
            'user_id',
            'hostname_id',
            'count'
    ]

    def __repr__(self):
        return 'UserHostname(user=%r, hostname=%r, count=%r)' \
                % (self.user_id, self.hostname_id, self.count)


class UserWordAdjacent(Row):
    """
    Adjacent words used by a given user
    """
    _TABLE_         = 'user_word_adjacent'
    _PRIMARY_KEYS_  = [
            'user_id',
            'proceeding_id',
            'following_id'
    ]
    _COLUMNS_       = [
            'user_id',
            'proceeding_id',
            'following_id',
            'count'
    ]

    def __repr__(self):
        return 'UserWordAdjacent(user=%r, proceeding=%r, '\
                'following=%r, count=%r)' \
                % (self.user_id, self.proceeding_id,
                        self.following_id,
                        self.count)


class UserToken(Row):
    """
    Suspect tokens found in regular expression search.
    """
    _TABLE_         = 'user_token'
    _PRIMARY_KEYS_  = ['user_id', 'token']
    _COLUMNS_       = [
            'user_id',
            'token',
            'count'
    ]


class Trait(Row):
    """
    Traits are characteristics which are common to subsets of users.
    """
    _TABLE_         = 'trait'
    _PRIMARY_KEYS_  = ['trait_id']
    _COLUMNS_       = [
            'trait_id',
            'trait_class',
            'trait_type',
            'score',
            'count',
            'weight'
    ]


class TraitInstance(Row):
    """
    Instances of particular traits (e.g. if the trait is a 'word',
    this may be the score for a particular word).
    """
    _TABLE_         = 'trait_instance'
    _PRIMARY_KEYS_  = ['trait_inst_id']
    _COLUMNS_       = [
            'trait_inst_id',
            'trait_id',
            'score',
            'count'
    ]

    @property
    def instance(self):
        return 't%di%d' % (self.trait_id, self.trait_inst_id)


class TraitInstanceString(TraitInstance):
    """
    Trait instance that is described by a string.
    """
    _COLUMNS_       = TraitInstance._COLUMNS_ + [
            'trait_string'
    ]

    @property
    def instance(self):
        return self.trait_string


class TraitInstanceAvatarHash(TraitInstance):
    """
    Trait instance that references an avatar hash.
    """
    _COLUMNS_       = TraitInstance._COLUMNS_ + [
            'trait_hash_id'
    ]

    @property
    def instance(self):
        return self.trait_hash_id


class TraitInstancePair(TraitInstance):
    """
    Instances of two other traits that appear together.
    """
    _COLUMNS_       = TraitInstance._COLUMNS_ + [
            'prev_id',
            'next_id'
    ]


class UserTrait(Row):
    """
    Traits linked to a particular user.
    """
    _TABLE_         = 'user_trait'
    _PRIMARY_KEYS_  = ['user_id', 'trait_id']
    _COLUMNS_       = [
            'user_id',
            'trait_id',
            'count'
    ]


class UserTraitInstance(Row):
    """
    Trait instances linked to a particular user.
    """
    _TABLE_         = 'user_trait_instance'
    _PRIMARY_KEYS_  = ['user_id', 'trait_inst_id']
    _COLUMNS_       = [
            'user_id',
            'trait_inst_id',
            'count'
    ]

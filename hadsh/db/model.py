from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import Index
from sqlalchemy.orm import relationship
from sqlalchemy import Column, BigInteger, String, ForeignKey, \
        LargeBinary, Text, DateTime, Table, Integer, Boolean
from sqlalchemy.dialects.postgresql import UUID

import base64

Base = declarative_base()


user_group_assoc = Table('user_group_assoc', Base.metadata,
    Column('user_id', BigInteger, ForeignKey('user.user_id')),
    Column('group_id', BigInteger, ForeignKey('group.group_id'))
)

user_tag_assoc = Table('user_tag_assoc', Base.metadata,
    Column('user_id', BigInteger, ForeignKey('user.user_id')),
    Column('tag_id', BigInteger, ForeignKey('tag.tag_id'))
)

avatar_hash_assoc = Table('avatar_hash_assoc', Base.metadata,
    Column('avatar_id', BigInteger, ForeignKey('avatar.avatar_id')),
    Column('hash_id', BigInteger, ForeignKey('avatar_hash.hash_id'))
)


class User(Base):
    """
    All recognised Hackaday.io users, including legitmate ones.
    """

    __tablename__   = 'user'

    user_id         = Column(BigInteger, primary_key=True)
    screen_name     = Column(String)
    url             = Column(String)
    avatar_id       = Column(BigInteger, ForeignKey('avatar.avatar_id'))
    created         = Column(DateTime(timezone=True))
    had_created     = Column(DateTime(timezone=True))
    last_update     = Column(DateTime(timezone=True))

    avatar = relationship("Avatar", back_populates="users")
    sessions = relationship("Session", back_populates="user",
            cascade="all, delete-orphan")
    links = relationship("UserLink", back_populates="user",
            cascade="all, delete-orphan")
    hostnames = relationship("UserHostname", back_populates="user",
            cascade="all, delete-orphan")
    words = relationship("UserWord", back_populates="user",
            cascade="all, delete-orphan")
    adj_words = relationship("UserWordAdjacent", back_populates="user",
            cascade="all, delete-orphan")
    tokens = relationship("UserToken", back_populates="user",
            cascade="all, delete-orphan")
    detail = relationship("UserDetail", uselist=False, back_populates="user",
            cascade="all, delete-orphan")
    groups = relationship("Group", secondary=user_group_assoc,
            back_populates="users")
    tags = relationship("Tag", secondary=user_tag_assoc,
            back_populates="users")

    def __repr__(self):
        return 'User(user_id=%r, screen_name=%r)' \
                % (self.user_id, self.screen_name)


class Account(Base):
    __tablename__   = 'account'
    user_id         = Column(BigInteger, ForeignKey('user.user_id'),
            primary_key=True)
    name            = Column(String, unique=True)
    hashedpassword  = Column(String)
    changenextlogin = Column(Boolean)


class DeferredUser(Base):
    """
    Object to represent when a user account has been added but inspection
    has been deferred.
    """

    __tablename__   = 'deferred_user'

    user_id         = Column(BigInteger, primary_key=True)
    inspect_time    = Column(DateTime(timezone=True))
    inspections     = Column(Integer)


class Group(Base):
    """
    Groups used for classifying users.
    """

    __tablename__   = 'group'

    group_id        = Column(BigInteger, primary_key=True)
    name            = Column(String, unique=True)

    users = relationship("User", secondary=user_group_assoc,
            back_populates="groups")

    def __repr__(self):
        return 'Group(group_id=%r, name=%r)' \
                % (self.group_id, self.name)


class Session(Base):
    """
    Session token storage.  The session ID will be emitted to the user, so
    we use a UUID field to make the value unguessable.
    """
    __tablename__   = 'session'

    session_id      = Column(UUID(as_uuid=True), primary_key=True)
    user_id         = Column(BigInteger, ForeignKey('user.user_id'))

    user            = relationship("User", back_populates="sessions")
    expiry_date     = Column(DateTime(timezone=True))

    def __repr__(self):
        return 'Session(user=%r)' \
                % (self.user)


class UserDetail(Base):
    """
    Detail on 'suspect' users.  Only users that have been identified as
    possibly spammy by the search algorithm, or users that have been flagged
    as spammy by logged-in users, appear here.
    """
    __tablename__   = 'user_detail'

    user_id         = Column(BigInteger, ForeignKey('user.user_id'),
                        primary_key=True)
    about_me        = Column(Text)
    who_am_i        = Column(Text)
    what_i_would_like_to_do = Column(Text)
    location        = Column(String)
    projects        = Column(Integer)

    user            = relationship("User", back_populates="detail")


class UserLink(Base):
    """
    Links attached to 'suspect' users.
    """
    __tablename__   = 'user_link'

    user_id         = Column(BigInteger, ForeignKey('user.user_id'),
                        primary_key=True)
    title           = Column(Text)
    url             = Column(String, primary_key=True)

    user            = relationship("User", back_populates="links")

    def __repr__(self):
        return 'UserLink(user=%r, title=%r, url=%r)' \
                % (self.user, self.title, self.url)


class Avatar(Base):
    """
    A cache of users' avatars, as some share the same image.
    """
    __tablename__   = 'avatar'

    avatar_id       = Column(BigInteger, primary_key=True)
    url             = Column(String, unique=True, index=True)
    avatar          = Column(LargeBinary)
    avatar_type     = Column(String)

    users           = relationship("User", back_populates="avatar")
    hashes = relationship("AvatarHash", secondary=avatar_hash_assoc,
            back_populates="avatars")


class AvatarHash(Base):
    """
    A hash of users' avatars and their scores.
    """
    __tablename__   = 'avatar_hash'

    hash_id         = Column(BigInteger, primary_key=True)
    hashalgo        = Column(String)
    hashdata        = Column(LargeBinary)

    score           = Column(BigInteger, nullable=False, default=0)
    count           = Column(BigInteger, nullable=False, default=0)

    @property
    def hashstr(self):
        return base64.a85encode(self.hashdata)

    avatars = relationship("Avatar", secondary=avatar_hash_assoc,
            back_populates="hashes")

    def __repr__(self):
        return '<%s #%d %s %s>' % (
                self.__class__.__name__,
                self.hash_id,
                self.hashalgo,
                self.hashstr)


Index('avatar_hash_index', AvatarHash.hashalgo, AvatarHash.hashdata)


class Tag(Base):
    """
    A list of tags seen applied to users' accounts.
    """
    __tablename__   = 'tag'

    tag_id          = Column(BigInteger, primary_key=True)
    tag             = Column(String, unique=True, index=True)

    users = relationship("User", secondary=user_tag_assoc,
            back_populates="tags")


class NewestUserPageRefresh(Base):
    """
    A record of when each page of the "newst users" list was last refreshed.
    """
    __tablename__   = 'newest_user_page_refresh'

    page_num        = Column(BigInteger, primary_key=True)
    refresh_date    = Column(DateTime(timezone=True))


class NewUser(Base):
    """
    A record of a new user that is to be inspected.
    """
    __tablename__   = 'new_user'

    user_id         = Column(BigInteger, primary_key=True)


class Hostname(Base):
    """
    A hostname that appears in the links of user profiles.
    """
    __tablename__   = 'hostname'

    hostname_id     = Column(BigInteger, primary_key=True)
    hostname        = Column(String, unique=True, index=True)
    score           = Column(BigInteger, nullable=False, default=0)
    count           = Column(BigInteger, nullable=False, default=0)

    def __repr__(self):
        return 'Hostname(hostname_id=%r, hostname=%r, score=%r, count=%r)' \
                % (self.hostname_id, self.hostname, self.score, self.count)


class Word(Base):
    """
    A single word in the vocabulary of the Hackaday.io community.
    """
    __tablename__   = 'word'

    word_id         = Column(BigInteger, primary_key=True)
    word            = Column(String, unique=True, index=True)
    score           = Column(BigInteger, nullable=False, default=0)
    count           = Column(BigInteger, nullable=False, default=0)

    def __repr__(self):
        return 'Word(word_id=%r, word=%r, score=%r, count=%r)' \
                % (self.word_id, self.word, self.score, self.count)


class WordAdjacent(Base):
    """
    How often two words appear next to each other.
    """
    __tablename__   = 'word_adjacent'

    proceeding_id   = Column(BigInteger, ForeignKey('word.word_id'),
                        primary_key=True, index=True)
    following_id    = Column(BigInteger, ForeignKey('word.word_id'),
                        primary_key=True, index=True)
    score           = Column(BigInteger, nullable=False, default=0)
    count           = Column(BigInteger, nullable=False, default=0)

    proceeding      = relationship("Word", foreign_keys=[proceeding_id])
    following       = relationship("Word", foreign_keys=[following_id])

    def __repr__(self):
        return 'WordAdjacent(proceeding=%r, following=%r, score=%r, count=%r)' \
                % (self.proceeding, self.following, self.score, self.count)


class UserWord(Base):
    """
    Words used by a given user
    """
    __tablename__   = 'user_word'

    user_id         = Column(BigInteger, ForeignKey('user.user_id'),
                        primary_key=True, index=True)
    word_id         = Column(BigInteger, ForeignKey('word.word_id'),
                        primary_key=True)
    count           = Column(BigInteger)

    user            = relationship("User", back_populates="words")
    word            = relationship("Word")

    def __repr__(self):
        return 'UserWord(user=%r, word=%r, count=%r)' \
                % (self.user, self.word, self.count)


class UserHostname(Base):
    """
    Hostnames used by a given user
    """
    __tablename__   = 'user_hostname'

    user_id         = Column(BigInteger, ForeignKey('user.user_id'),
                        primary_key=True, index=True)
    hostname_id     = Column(BigInteger, ForeignKey('hostname.hostname_id'),
                        primary_key=True)
    count           = Column(BigInteger)

    user            = relationship("User", back_populates="hostnames")
    hostname        = relationship("Hostname")

    def __repr__(self):
        return 'UserHostname(user=%r, hostname=%r, count=%r)' \
                % (self.user, self.hostname, self.count)


class UserWordAdjacent(Base):
    """
    Adjacent words used by a given user
    """
    __tablename__   = 'user_word_adjacent'

    user_id         = Column(BigInteger, ForeignKey('user.user_id'),
                        primary_key=True, index=True)
    proceeding_id   = Column(BigInteger, ForeignKey('word.word_id'),
                        primary_key=True)
    following_id    = Column(BigInteger, ForeignKey('word.word_id'),
                        primary_key=True)
    count           = Column(BigInteger)

    user            = relationship("User", back_populates="adj_words")
    proceeding      = relationship("Word", foreign_keys=[proceeding_id])
    following       = relationship("Word", foreign_keys=[following_id])

    def __repr__(self):
        return 'UserWordAdjacent(user=%r, proceeding=%r, '\
                'following=%r, count=%r)' \
                % (self.user, self.proceeding, self.following,
                        self.count)


class UserToken(Base):
    """
    Suspect tokens found in regular expression search.
    """
    __tablename__   = 'user_token'
    user_id         = Column(BigInteger, ForeignKey('user.user_id'),
                        primary_key=True, index=True)
    token           = Column(String, primary_key=True)
    count           = Column(BigInteger)

    user            = relationship("User", back_populates="tokens")


class Trait(Base):
    """
    Traits are characteristics which are common to subsets of users.
    """
    __tablename__   = 'trait'

    trait_id        = Column(BigInteger, primary_key=True)
    trait_class     = Column(String, index=True)
    trait_type      = Column(String)
    score           = Column(BigInteger, nullable=False, default=0)
    count           = Column(BigInteger, nullable=False, default=0)


class TraitInstance(Base):
    """
    Instances of particular traits (e.g. if the trait is a 'word',
    this may be the score for a particular word).
    """
    __tablename__   = 'trait_instance'

    trait_inst_id   = Column(BigInteger, primary_key=True)
    trait_id        = Column(BigInteger, ForeignKey('trait.trait_id'))
    score           = Column(BigInteger, nullable=False, default=0)
    count           = Column(BigInteger, nullable=False, default=0)


class TraitInstanceString(TraitInstance):
    """
    Trait instance that is described by a string.
    """
    trait_string    = Column(String)
Index('trait_instance_string_index',
        TraitInstance.trait_id, TraitInstanceString.trait_string)


class TraitInstanceAvatarHash(TraitInstance):
    """
    Trait instance that references an avatar hash.
    """
    trait_hash_id   = Column(BigInteger, ForeignKey('avatar_hash.hash_id'))
Index('trait_instance_avatar_hash_index',
        TraitInstance.trait_id, TraitInstanceAvatarHash.trait_hash_id)


class UserTraits(Base):
    """
    Traits linked to a particular user.
    """
    user_id         = Column(BigInteger, ForeignKey('user.user_id'),
                        primary_key=True, index=True)
    trait_id        = Column(BigInteger, ForeignKey('trait.trait_id'),
                        primary_key=True)
    count           = Column(BigInteger, nullable=False, default=0)


class UserTraitInstances(Base):
    """
    Trait instances linked to a particular user.
    """
    user_id         = Column(BigInteger, ForeignKey('user.user_id'),
                        primary_key=True, index=True)
    trait_inst_id   = Column(BigInteger,
                        ForeignKey('trait_instance.trait_inst_id'),
                        primary_key=True)
    count           = Column(BigInteger, nullable=False, default=0)

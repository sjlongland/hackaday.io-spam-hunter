from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, BigInteger, String, ForeignKey, \
        Boolean, LargeBinary, Text, DateTime
from sqlalchemy.dialects.postgresql import UUID

Base = declarative_base()

class User(Base):
    """
    All recognised Hackaday.io users, including legitmate ones.
    """

    __tablename__   = 'user'

    user_id         = Column(BigInteger, primary_key=True)
    screen_name     = Column(String)
    url             = Column(String)
    avatar_id       = Column(BigInteger, ForeignKey('avatar.avatar_id'))
    last_update     = Column(DateTime(timezone=True))


class Group(Base):
    """
    Groups used for classifying users.
    """

    __tablename__   = 'group'

    group_id        = Column(BigInteger, primary_key=True)
    name            = Column(String, unique=True)


class GroupMember(Base):
    """
    Group membership links.
    """
    __tablename__   = 'group_member'

    group_id        = Column(BigInteger, ForeignKey('group.group_id'),
                        primary_key=True, index=True)
    user_id         = Column(BigInteger, ForeignKey('user.user_id'),
                        primary_key=True, index=True)


class Session(Base):
    """
    Session token storage.  The session ID will be emitted to the user, so
    we use a UUID field to make the value unguessable.
    """
    __tablename__   = 'session'

    session_id      = Column(UUID(as_uuid=True), primary_key=True)
    user_id         = Column(BigInteger, ForeignKey('user.user_id'))
    token           = Column(String)

    user            = relationship("User", back_populates="sessions")


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

    user            = relationship("User", back_populates="detail")


class UserLink(Base):
    """
    Links attached to 'suspect' users.
    """
    __tablename__   = 'user_link'

    user_id         = Column(BigInteger, ForeignKey('user.user_id'),
                        primary_key=True)
    title           = Column(Text)
    location        = Column(String)
    url             = Column(String)

    user            = relationship("User", back_populates="links")


class Avatar(Base):
    """
    A cache of users' avatars, as some share the same image.
    """
    __tablename__   = 'avatar'

    avatar_id       = Column(BigInteger, primary_key=True)
    url             = Column(String, unique=True, index=True)
    avatar          = Column(LargeBinary)
    avatar_type     = Column(String)


class Tag(Base):
    """
    A list of tags seen applied to users' accounts.
    """
    __tablename__   = 'tag'

    tag_id          = Column(BigInteger, primary_key=True)
    tag             = Column(String, unique=True, index=True)


class UserTag(Base):
    """
    A list of tags applied to a user's account.
    """
    __tablename__   = 'user_tag'

    user_id         = Column(BigInteger, ForeignKey('user.user_id'),
                        primary_key=True)
    tag_id          = Column(BigInteger, ForeignKey('tag.tag_id'),
                        primary_key=True)

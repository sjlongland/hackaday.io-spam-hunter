from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, BigInteger, String, ForeignKey, \
        Boolean, LargeBinary, Text, DateTime, Table
from sqlalchemy.dialects.postgresql import UUID

Base = declarative_base()


user_group_assoc = Table('user_group_assoc', Base.metadata,
    Column('user_id', BigInteger, ForeignKey('user.user_id')),
    Column('group_id', BigInteger, ForeignKey('group.group_id'))
)

user_tag_assoc = Table('user_tag_assoc', Base.metadata,
    Column('user_id', BigInteger, ForeignKey('user.user_id')),
    Column('tag_id', BigInteger, ForeignKey('tag.tag_id'))
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
    last_update     = Column(DateTime(timezone=True))

    sessions = relationship("Session", back_populates="user",
            cascade="all, delete-orphan")
    links = relationship("UserLink", back_populates="user",
            cascade="all, delete-orphan")
    detail = relationship("UserDetail", uselist=False, back_populates="user",
            cascade="all, delete-orphan")
    groups = relationship("Group", secondary=user_group_assoc,
            back_populates="users")
    tags = relationship("Tag", secondary=user_tag_assoc,
            back_populates="users")


class Group(Base):
    """
    Groups used for classifying users.
    """

    __tablename__   = 'group'

    group_id        = Column(BigInteger, primary_key=True)
    name            = Column(String, unique=True)

    users = relationship("User", secondary=user_group_assoc,
            back_populates="groups")


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

    users = relationship("User", secondary=user_tag_assoc,
            back_populates="tags")


class NewestUserPageRefresh(Base):
    """
    A record of when each page of the "newst users" list was last refreshed.
    """
    __tablename__   = 'newest_user_page_refresh'

    page_num        = Column(BigInteger, primary_key=True)
    refresh_date    = Column(DateTime(timezone=True))

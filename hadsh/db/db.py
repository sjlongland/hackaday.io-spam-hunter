from .model import Base, User, Group, GroupMember, Session, UserDetail, \
        UserLink, Avatar, Tag, UserTag
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker


def get_db(db_uri, **kwargs):
    """
    Retrieve an instance of the back-end database
    """
    engine = create_engine(db_uri, **kwargs)
    Base.metadata.create_all(engine)
    return scoped_session(sessionmaker(bind=engine))
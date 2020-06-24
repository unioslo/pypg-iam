# Example of usage:
# from sqlalchemy import create_engine
# from sqlalchemy.orm import scoped_session, sessionmaker
# from iam.database import schema_utils
# engine = create_engine(
#    'postgresql://iam:secret@postgres:5432/iam',
#    convert_unicode=True)
#
# db_session = scoped_session(sessionmaker(autocommit=False,
#                                          autoflush=False,
#                                          bind=engine))
# schema_utils.create_schema()

import logging

from iam.database.models import Base

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


def create_schema(engine):
    """
    Create all database objects.
    """
    Base.metadata.create_all(bind=engine)


def drop_schema(engine):
    """
    Drop all database objects.
    """
    Base.metadata.drop_all(bind=engine)


def reset_schema(engine):
    """
    Drop and recreate database objects.
    """
    drop_schema(engine)
    create_schema(engine)

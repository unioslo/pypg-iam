
from contextlib import contextmanager

from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base


@contextmanager
def session_scope(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


class Db(object):


    """Reflect the pgi-iam database, and provide
    helper methods for database functions."""


    def __init__(self, engine):
        super(Db, self).__init__()
        self.engine = engine
        self.meta = MetaData()
        self.meta.reflect(bind=engine)
        self.persons = self.meta.tables['persons']
        self.users = self.meta.tables['users']
        self.groups = self.meta.tables['groups']
        self.group_memberships = self.meta.tables['group_memberships']
        self.group_moderators = self.meta.tables['group_moderators']
        self.capabilities_http = self.meta.tables['capabilities_http']
        self.capabilities_http_grants = self.meta.tables['capabilities_http_grants']


    def exec_sql(self, sql, params={}):
        """
        Execute a parameterised SQL query as a prepated statement,
        fetching all results.

        Parameters
        ----------
        sql: str
        params: dict

        Example
        -------
        exec_sql('select * from persons where name=:name', {'name': 'Frank'})
        exec_sql('select * from users')

        Returns
        -------
        list of tuples

        """
        with session_scope(self.engine) as session:
            data = session.execute(sql, params).fetchall()
        return data


    # TODO
    # add the rest of the functions

    def group_members(self, group_name):
        q = "select group_members('{0}')".format(group_name)
        return self.exec_sql(q)[0][0]


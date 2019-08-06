
from contextlib import contextmanager

from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker


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


    """
    Reflect the pgi-iam database to sqlalchemy objects,
    provide helper methods for calling database functions,
    and executing arbitrary SQL queries.

    Tables
    ------
    persons
    users
    groups
    group_memberships
    group_moderators
    capabilities_http
    capabilities_http_grants

    Functions
    ---------
    person_groups
    person_capabilities
    person_access
    user_groups
    user_capabilities
    group_members
    group_moderators
    group_member_add
    group_member_remove
    group_capabilities
    capability_grants

    """


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


    def person_groups(self, person_id):
        """
        Get the group memberships associated with a person's
        person group.

        Parameters
        ----------
        person_id: str, uuid4

        Returns
        -------
        dict

        """
        q = "select person_groups('{0}')".format(person_id)
        return self.exec_sql(q)[0][0]


    def person_capabilities(self, person_id, grants=True):
        """
        Get an overview of the capabilities a person has access to
        via their group memberships.

        Parameters
        ----------
        person_id, str, uuid4
        grants: bool, default=True (also show capability resource grants)

        Returns
        -------
        dict

        """
        g = 't' if grants else 'f'
        q = "select person_capabilities('{0}', '{1}')".format(person_id, g)
        return self.exec_sql(q)[0][0]


    def person_access(self, person_id):
        """
        Get an overview of all access rights the person has,
        via their person group, and all the user accounts, and
        user groups linked to those accounts.

        Parameters
        ----------
        person_id, str, uuid4

        Returns
        -------
        dict

        """
        q = "select person_access('{0}')".format(person_id)
        return self.exec_sql(q)[0][0]


    def group_members(self, group_name):
        """
        Get the membership graph of group_name.

        Parameters
        ----------
        group_name: str

        Returns
        -------
        dict

        """
        q = "select group_members('{0}')".format(group_name)
        return self.exec_sql(q)[0][0]


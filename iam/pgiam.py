
"""This package provides a Db class, which is a thin wrapper around the pg-iam
database system. The class provides sqlalchemy objects, and instance methods
for calling database functions."""


from contextlib import contextmanager
from collections import namedtuple

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

    Example usage
    -------------
    engine = sqlalchemy.create_engine(...)
    db = Db(engine)
    query = 'select person_id from persons where name=:name'
    pid = db.exec_sql(query, {'name': 'Catullus'})[0][0]
    pgrps = db.person_groups(pid)
    query = 'select user_name from users where person_id=:pid'
    user_name = db.exec_sql(query, {'pid': pid})[0][0]
    ugrps = db.user_groups(user_name)
    db.group_member_add('admin', user_name)

    # one can also use sqlalchemy tables for select, insert, update and delete
    from sqlalchemy.sql import select
    users = db.tables.users
    results = conn.execute(select([users])).fetchall()

    # for insert, update and delete plain parameterised SQL is also just fine
    vals = {'g': 'g1', 'm': 'g2'}
    db.exec_sql('insert into group_moderators values (:g, :m)', vals, fetch=False)

    """


    def __init__(self, engine):
        super(Db, self).__init__()
        self.engine = engine
        self.meta = MetaData()
        self.meta.reflect(bind=engine)
        self.tables = namedtuple('tables', ['persons', 'users', 'groups',
                                            'group_memberships', 'group_moderators',
                                            'capabilities_http', 'capabilities_http_grants'])
        self.tables.persons = self.meta.tables['persons']
        self.tables.users = self.meta.tables['users']
        self.tables.groups = self.meta.tables['groups']
        self.tables.group_memberships = self.meta.tables['group_memberships']
        self.tables.group_moderators = self.meta.tables['group_moderators']
        self.tables.capabilities_http = self.meta.tables['capabilities_http']
        self.tables.capabilities_http_grants = self.meta.tables['capabilities_http_grants']


    def exec_sql(self, sql, params={}, fetch=True):
        """
        Execute a parameterised SQL query as a prepated statement,
        fetching all results.

        Parameters
        ----------
        sql: str
        params: dict
        fetch: bool, set to False for insert, update and delte

        Example
        -------
        exec_sql('select * from persons where name=:name', {'name': 'Frank'})
        exec_sql('select * from users')
        exec_sql('insert into mytable values (:y)', {'y': 5}, fetch=False)

        Returns
        -------
        list of tuples or boolean

        """
        res = True
        with session_scope(self.engine) as session:
            data = session.execute(sql, params)
            if fetch:
                res = data.fetchall()
        return res


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


    def user_groups(self, user_name):
        """
        Get the group memberships for a user.

        Parameters
        ----------
        user_name: str

        Returns
        -------
        dict

        """
        q = "select user_groups('{0}')".format(user_name)
        return self.exec_sql(q)[0][0]


    def user_capabilities(self, user_name, grants=True):
        """
        Get the capabilities (access) for a user via its group
        memberships.

        Parameters
        ----------
        user_name: str
        grants: bool, default=True (also show capability resource grants)

        Returns
        -------
        dict

        """
        g = 't' if grants else 'f'
        q = "select user_capabilities('{0}', '{1}')".format(user_name, g)
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


    def group_moderators(self, group_name):
        """
        Get the moderators for a group.

        Parameters
        ----------
        group_name: str

        Returns
        -------
        dict

        """
        q = "select group_moderators('{0}')".format(group_name)
        return self.exec_sql(q)[0][0]


    def group_member_add(self, group_name, member):
        """
        Add a new member to a group. A new member can be identified
        by either:

        1) person_id
        2) user_name
        3) group (person group, user group, or generic group)

        If a new member is identified using #1, then pg-iam
        will find their person group, and add it as a member.
        If #2 is used, then pg-iam will find the user group and
        add it as a member. In case #3, if a person or user group
        is given, then it is functionally equivalent to #1 and #2.
        When a generic group is provided, then the group becomes
        a member of another group (along with its members, transitively).

        Note: internally, pg-iam adds persons to groups via their
        person group, and users to groups via their user groups.

        Parameters
        ----------
        group_name: str, the group to which the member should be added
        member: str, the new member

        Returns
        -------
        dict

        """
        q = "select group_member_add('{0}', '{1}')".format(group_name, member)
        return self.exec_sql(q)[0][0]


    def group_member_remove(self, group_name, member):
        """
        Remove a member from a group. A member can be identified
        by either:

        1) person_id
        2) user_name
        3) group (person group, user group, or generic group)

        Parameters
        ----------
        group_name: str, the group from which the member should be removed
        member: str, the existing member to remove

        Returns
        -------
        dict

        """
        q = "select group_member_remove('{0}', '{1}')".format(group_name, member)
        return self.exec_sql(q)[0][0]


    def group_capabilities(self, group_name, grants=True):
        """
        Get the capabilities that the group enables access to.

        Parameters
        ----------
        group_name: str
        grants: bool, default=True (also show capability resource grants)

        Returns
        -------
        dict

        """
        g = 't' if grants else 'f'
        q = "select group_capabilities('{0}', '{1}')".format(group_name, g)
        return self.exec_sql(q)[0][0]


    def capability_grants(self, capability_name):
        """
        Get the resource grants associated with a specific capability.

        Parameters
        ----------
        capability_name: str

        Returns
        -------
        dict

        """
        q = "select capability_grants('{0}')".format(capability_name)
        return self.exec_sql(q)[0][0]


"""This package provides a Db class, which is a thin wrapper around the pg-iam
database system. The class provides sqlalchemy objects, and instance methods
for calling database functions."""


from contextlib import contextmanager
from collections import namedtuple

from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker


@contextmanager
def session_scope(engine, session_identity=None):
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        if session_identity:
            q = 'set session "session.identity" = \'{0}\''.format(session_identity)
            session.execute(q)
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
    audit_log_objects
    audit_log_relations

    Note: the audit_log_objects, and audit_log_relations are partitioned
    by the table_name column, so it is recommended that queries _always_
    filter on 'where table_name = name' when doing select queries.

    Functions
    ---------
    person_groups
    person_capabilities
    person_access
    user_groups
    user_moderators
    user_capabilities
    group_members
    group_moderators
    group_member_add
    group_member_remove
    group_capabilities
    capability_grants
    capability_grant_rank_set
    capability_grant_delete
    capability_instance_get
    capabilities_http_sync
    capabilities_http_grants_sync

    Example usage
    -------------
    from sqlalchemy import create_engine
    from sqlalchemy.pool import QueuePool

    from iam.pgiam import Db, session_scope

    engine = create_engine(dburi, poolclass=QueuePool)
    db = Db(engine)

    # use raw sql and helper functions
    query = 'select person_id from persons where name=:name'
    pid = db.exec_sql(query, {'name': 'Catullus'})[0][0]
    pgrps = db.person_groups(pid)
    query = 'select user_name from users where person_id=:pid'
    user_name = db.exec_sql(query, {'pid': pid})[0][0]
    ugrps = db.user_groups(user_name)
    db.group_member_add('admin', user_name)
    vals = {'g': 'g1', 'm': 'g2'}
    db.exec_sql('insert into group_moderators values (:g, :m)', vals, fetch=False)

    # use sqlalchemy tables for select, insert, update and delete
    identity = 'random_person'
    with session_scope(db.engine, identity) as session:
        for person in session.query(db.tables.persons):
            print(person)

    # How to Insert
    insert = db.tables.persons.insert().values(full_name="Milen Kouylekov").execute()

    # How to Count
    db.tables.persons.count().execute().next()[0]

    # How to Search
    db.tables.persons.select().where(db.tables.persons.columns.full_name == "Milen Kouylekov").execute().next()

    # Update
    db.tables.persons.update().where(db.tables.persons.columns.full_name == 'Milen Kouylekov').values(full_name='TSD Admin').execute()

    # Execute
    with session_scope(db.engine, identity) as session:
        stmt = db.tables.persons.update().where(db.tables.persons.columns.full_name == 'Milen Kouylekov').values(full_name='TSD Admin')
        session.execute(stmt)

    # Delete
    db.tables.persons.delete().where(db.tables.persons.columns.full_name == 'TSD Admin').execute()

    # Next vs fetch One
    db.tables.persons.select().where(db.tables.persons.columns.full_name == 'TSD Admin').execute().next() Throws StopIteration if not found
    db.tables.persons.select().where(db.tables.persons.columns.full_name == 'TSD Admin').execute().fetch_one() returns None if not found



    """

    def __init__(self, engine):
        super(Db, self).__init__()
        self.engine = engine
        self.meta = MetaData(engine)
        self.meta.reflect()
        self.tables = namedtuple('tables', ['persons', 'users', 'groups',
                                            'group_memberships', 'group_moderators',
                                            'capabilities_http', 'capabilities_http_grants',
                                            'audit_log_objects', 'audit_log_relations'])
        self.tables.persons = self.meta.tables['persons']
        self.tables.users = self.meta.tables['users']
        self.tables.groups = self.meta.tables['groups']
        self.tables.group_memberships = self.meta.tables['group_memberships']
        self.tables.group_moderators = self.meta.tables['group_moderators']
        self.tables.capabilities_http = self.meta.tables['capabilities_http']
        self.tables.capabilities_http_instances = self.meta.tables['capabilities_http_instances']
        self.tables.capabilities_http_grants = self.meta.tables['capabilities_http_grants']
        self.tables.audit_log_objects = self.meta.tables['audit_log_objects']
        self.tables.audit_log_relations = self.meta.tables['audit_log_objects']

    def exec_sql(self, sql, params={}, fetch=True, session_identity=None):
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
        with session_scope(self.engine, session_identity) as session:
            data = session.execute(sql, params)
            if fetch:
                res = data.fetchall()
        return res

    def person_groups(self, person_id, session_identity=None):
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
        return self.exec_sql(q, session_identity=session_identity)[0][0]

    def person_capabilities(self, person_id, grants=True, session_identity=None):
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
        return self.exec_sql(q, session_identity=session_identity)[0][0]

    def person_access(self, person_id, session_identity=None):
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
        return self.exec_sql(q, session_identity=session_identity)[0][0]

    def user_groups(self, user_name, session_identity=None):
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
        return self.exec_sql(q, session_identity=session_identity)[0][0]

    def user_moderators(self, user_name, session_identity=None):
        """
        Get the groups which the user moderates.

        Parameters
        ----------
        user_name: str

        Returns
        -------
        dict

        """
        q = "select user_moderators('{0}')".format(user_name)
        return self.exec_sql(q, session_identity=session_identity)[0][0]

    def user_capabilities(self, user_name, grants=True, session_identity=None):
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
        return self.exec_sql(q, session_identity=session_identity)[0][0]

    def group_members(self, group_name, session_identity=None):
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
        return self.exec_sql(q, session_identity=session_identity)[0][0]

    def group_moderators(self, group_name, session_identity=None):
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
        return self.exec_sql(q, session_identity=session_identity)[0][0]

    def group_member_add(self, group_name, member, session_identity=None):
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
        return self.exec_sql(q, session_identity=session_identity)[0][0]

    def group_member_remove(self, group_name, member, session_identity=None):
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
        return self.exec_sql(q, session_identity=session_identity)[0][0]

    def group_capabilities(self, group_name, grants=True, session_identity=None):
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
        return self.exec_sql(q, session_identity=session_identity)[0][0]

    def capability_grants(self, capability_name, session_identity=None):
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
        return self.exec_sql(q, session_identity=session_identity)[0][0]

    def capability_grant_rank_set(self, grant_id, new_grant_rank, session_identity=None):
        """
        Set the rank of a grant.

        Parameters
        ----------
        grant_id: str (uuid4)
        new_grant_rank: int

        Returns
        -------
        bool

        """
        q = "select capability_grant_rank_set('{0}', '{1}')".format(grant_id, new_grant_rank)
        return self.exec_sql(q, session_identity=session_identity)[0][0]

    def capability_grant_delete(self, grant_id, session_identity=None):
        """
        Get the resource grants associated with a specific capability.

        Parameters
        ----------
        grant_id: str (uuid4)

        Returns
        -------
        bool

        """
        q = "select capability_grant_delete('{0}')".format(grant_id)
        return self.exec_sql(q, session_identity=session_identity)[0][0]

    def capability_instance_get(self, instance_id, session_identity=None):
        """
        Create a capability instance.

        Parameters
        ----------
        instance_id: str (uuid4)

        Returns
        -------
        dict

        """
        q = "select capability_instance_get('{0}')".format(instance_id)
        return self.exec_sql(q, session_identity=session_identity)[0][0]

    def capabilities_http_sync(self, capabilities, session_identity=None):
        """
        Synchronise a list of capabilities to the capabilities_http table,
        replacing any existing entries with the same names, and adding
        any entries which do not exist. There is no auto deletion - since
        that would cascade to grants.

        Semantics: over-write or append, atomically.

        NB!: for any given capability name provided by the caller,
        if the entry exists, if existing columns in the db have values,
        but are not set in the call, they will be set to NULL.

        For example, if you have an entry such as:

        capability_name | capability_default_claims | ...
        ---------------   -------------------------   ---
        test            | {'user': 'test-user'}

        And the caller provides a capability in the capabilities
        parameter such as:

        [{'capability_name': 'test', ...}], omitting the capability_default_claims
        from the keys, then the result of the sync will be:

        capability_name | capability_default_claims | ...
        ---------------   -------------------------   ---
        test            | NULL

        The caller should, therefore, take care to fully specify the
        capabilities that will be synced, and not rely on existing
        information in the db.

        Parameters
        ----------
        capabilities: list of dicts

        The following dict keys are compulsory:
            capability_name: str
            capability_required_groups: list
            capability_lifetime: str
            capability_desription: str

        For a full list of available columns, see:

            db.tables.capabilities_http

        Example usage
        -------------
        names = [
            {
                'capability_name': 'import',
                'capability_required_groups': ['some-group'],
                'capability_lifetime': 20,
                'capability_desription': 'allow import'
            },
            {
                'capability_name': 'export',
                'capability_required_groups': ['another-group', 'super-group'],
                'capability_lifetime': 10,
                'capability_desription': 'allow export'
            },
        ]
        db.capabilities_http_sync_names(names)

        Returns
        -------
        bool

        """
        res = True
        required_keys = ['capability_name', 'capability_required_groups',
                         'capability_lifetime', 'capability_description']
        for capability in capabilities:
            input_keys = capability.keys()
            for key in required_keys:
                if key not in input_keys:
                    m = 'missing required key: {0} in capability, cannot do sync without error'.format(key)
                    raise Exception(m)
        table_columns = list(map(lambda x: str(x).replace('capabilities_http.', ''),
                                 self.tables.capabilities_http.columns))[2:]
        with session_scope(self.engine, session_identity) as session:
            for capability in capabilities:
                exists_query = 'select count(1) from capabilities_http where capability_name = :capability_name'
                exists = session.execute(exists_query, capability).fetchone()[0]
                input_keys = capability.keys()
                for column in table_columns:
                    if column not in input_keys:
                        capability[column] = None
                if exists:
                    update_query = """
                        update capabilities_http set
                            capability_default_claims = :capability_default_claims,
                            capability_required_groups = :capability_required_groups,
                            capability_required_attributes = :capability_required_attributes,
                            capability_group_match_method = :capability_group_match_method,
                            capability_lifetime = :capability_lifetime,
                            capability_description = :capability_description,
                            capability_expiry_date = :capability_expiry_date,
                            capability_group_existence_check = :capability_group_existence_check,
                            capability_metadata = :capability_metadata
                        where capability_name = :capability_name"""
                    session.execute(update_query, capability)
                else:
                    insert_query = """
                        insert into capabilities_http
                            (capability_name, capability_default_claims,
                             capability_required_groups, capability_required_attributes,
                             capability_group_match_method, capability_lifetime,
                             capability_description, capability_expiry_date,
                             capability_group_existence_check, capability_metadata)
                          values
                            (:capability_name, :capability_default_claims,
                             :capability_required_groups, :capability_required_attributes,
                             :capability_group_match_method, :capability_lifetime,
                             :capability_description, :capability_expiry_date,
                             :capability_group_existence_check, :capability_metadata)"""
                    session.execute(insert_query, capability)
        return res

    def capabilities_http_grants_sync(self, grants, session_identity=None):
        """
        Synchronise a list of grants to the capabilities_http_grants table,
        explicitly by capability_grant_id. The caller MUST provide IDs.
        Although generating UUIDs may seem laborious, it is the only way
        to ensure the sync is 100% correct, given the dynamic generation of
        grants.

        Semantics: over-write or append, atomically.

        Parameters
        ----------
        grants: list of dicts

        The following dict keys are compulsory:
            capability_name: str
            capability_grant_id: uuid4
            capability_grant_hostname: str
            capability_grant_namespace: str
            capability_grant_http_method: str
            capability_grant_rank: int > 0
            capability_grant_uri_pattern: str
            capability_grant_required_groups: list

        Returns
        -------
        bool

        """
        res = True
        required_keys = ['capability_name', 'capability_grant_id',
                         'capability_grant_hostname', 'capability_grant_namespace',
                         'capability_grant_http_method', 'capability_grant_rank',
                         'capability_grant_uri_pattern', 'capability_grant_required_groups']
        for capability in grants:
            input_keys = grants.keys()
            for key in required_keys:
                if key not in input_keys:
                    m = 'missing required key: {0} in grant, cannot do sync without error'.format(key)
                    raise Exception(m)
        table_columns = list(map(lambda x: str(x).replace('capabilities_http_grants.', ''),
                                 self.tables.capabilities_http_grants.columns))[2:]
        with session_scope(self.engine, session_identity) as session:
            for grant in grants:
                exists_query = """select count(1) from capabilities_http_grants
                                  where capability_grant_id = :capability_grant_id"""
                exists = session.execute(exists_query, grant).fetchone()[0]
                input_keys = capability.keys()
                for column in table_columns:
                    if column not in input_keys:
                        grant[column] = None
                if exists:
                    # update everything but the rank
                    print('exists')
                    update_query = """
                        update capabilities_http_grants set
                            capability_name = :capability_name,
                            capability_grant_hostname = :capability_grant_hostname,
                            capability_grant_namespace = :capability_grant_namespace,
                            capability_grant_http_method = :capability_grant_http_method,
                            capability_grant_uri_pattern = :capability_grant_uri_pattern,
                            capability_grant_required_groups = :capability_grant_required_groups,
                            capability_grant_required_attributes = :capability_grant_required_attributes,
                            capability_grant_start_date = :capability_grant_start_date,
                            capability_grant_end_date = :capability_grant_end_date,
                            capability_grant_max_num_usages = :capability_grant_max_num_usages,
                            capability_grant_group_existence_check = :capability_group_existence_check,
                            capability_grant_metadata = :capability_grant_metadata
                        where capability_grant_id = :capability_grant_id"""
                    session.execute(update_query, grant)
                    session.execute("select capability_grant_rank_set('{0}', '{1}')".format(
                        grant['capability_grant_id'], grant['capability_grant_rank']))
                else:
                    insert_query = """
                        insert into capabilities_http_grants
                            (capability_name,
                             capability_grant_hostname,
                             capability_grant_namespace,
                             capability_grant_http_method,
                             capability_grant_uri_pattern,
                             capability_grant_required_groups,
                             capability_grant_required_attributes,
                             capability_grant_start_date,
                             capability_grant_end_date,
                             capability_grant_max_num_usages,
                             capability_grant_group_existence_check,
                             capability_grant_metadata)
                        values
                            (:capability_name,
                             :capability_grant_hostname,
                             :capability_grant_namespace,
                             :capability_grant_http_method,
                             :capability_grant_uri_pattern,
                             :capability_grant_required_groups,
                             :capability_grant_required_attributes,
                             :capability_grant_start_date,
                             :capability_grant_end_date,
                             :capability_grant_max_num_usages,
                             :capability_grant_group_existence_check,
                             :capability_grant_metadata)"""
                    session.execute(insert_query, grant)
                    session.execute("select capability_grant_rank_set('{0}', '{1}')".format(
                        grant['capability_grant_id'], grant['capability_grant_rank']))
        return res

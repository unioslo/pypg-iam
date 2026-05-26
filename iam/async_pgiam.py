"""This package provides an AsyncDb class, which is a thin async wrapper around the
pg-iam database system. The class provides async methods for calling database functions.

This is the async version of the Db class from pgiam.py, using SQLAlchemy's async support."""

import json

from contextlib import asynccontextmanager
from typing import Union, Optional, AsyncContextManager

import sqlalchemy

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from ._constants import (
    # Validation constants
    CAPABILITIES_REQUIRED_KEYS,
    CAPABILITIES_JSON_COLUMNS,
    CAPABILITIES_TABLE_COLUMNS,
    GRANTS_REQUIRED_KEYS,
    GRANTS_JSON_COLUMNS,
    GRANTS_TABLE_COLUMNS,
    # Complex SQL queries
    CAPABILITIES_HTTP_UPDATE_QUERY,
    CAPABILITIES_HTTP_INSERT_QUERY,
    CAPABILITIES_HTTP_DELETE_QUERY,
    GRANTS_EXISTS_QUERY,
    GRANTS_GET_ID_FROM_NAME_QUERY,
    GRANTS_UPDATE_QUERY,
    GRANTS_INSERT_QUERY,
    GRANTS_FIND_EXISTING_QUERY,
    # Database function queries
    QUERY_PERSON_GROUPS,
    QUERY_PERSON_CAPABILITIES,
    QUERY_PERSON_ACCESS,
    QUERY_USER_GROUPS,
    QUERY_USER_MODERATORS,
    QUERY_USER_CAPABILITIES,
    QUERY_GROUP_MEMBERS,
    QUERY_GROUP_MODERATORS,
    QUERY_GROUP_MEMBER_ADD,
    QUERY_GROUP_MEMBER_REMOVE,
    QUERY_GROUP_CAPABILITIES,
    QUERY_INSTITUTION_GROUP_ADD,
    QUERY_INSTITUTION_GROUP_REMOVE,
    QUERY_INSTITUTION_GROUPS,
    QUERY_INSTITUTION_MEMBER_ADD,
    QUERY_INSTITUTION_MEMBER_REMOVE,
    QUERY_INSTITUTION_MEMBERS,
    QUERY_PROJECT_GROUP_ADD,
    QUERY_PROJECT_GROUP_REMOVE,
    QUERY_PROJECT_GROUPS,
    QUERY_PROJECT_INSTITUTIONS,
    QUERY_CAPABILITIES_HTTP_LIST,
    QUERY_CAPABILITY_GRANT_RANK_SET,
    QUERY_CAPABILITY_GRANT_DELETE,
    QUERY_CAPABILITY_GRANTS_DELETE,
    QUERY_CAPABILITY_INSTANCE_GET,
    QUERY_CAPABILITY_GRANT_GROUP_ADD,
    QUERY_CAPABILITY_GRANT_GROUP_REMOVE,
)
from ._util import (
    # Utilities
    dsn_from_config,
    with_all_http_methods,
)


def async_iam_engine(dsn: str, require_ssl: bool = False) -> sqlalchemy.ext.asyncio.AsyncEngine:
    # Convert postgresql:// to postgresql+asyncpg:// for async support
    if dsn.startswith("postgresql://"):
        dsn = dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
    connect_args = {} if not require_ssl else {"ssl": "require"}
    engine = create_async_engine(dsn, connect_args=connect_args, pool_size=10, max_overflow=0)
    return engine


@asynccontextmanager
async def async_session_scope(
    engine: sqlalchemy.ext.asyncio.AsyncEngine,
    session_identity: Optional[str] = None,
    session: Optional[AsyncSession] = None,
) -> AsyncContextManager[AsyncSession]:
    SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = SessionLocal()
    try:
        if session_identity:
            q = "set session \"session.identity\" = '{0}'".format(session_identity)
            await session.execute(sqlalchemy.text(q))
        yield session
        await session.commit()
    except Exception as e:
        await session.rollback()
        raise e
    finally:
        await session.close()


class AsyncDb(object):
    """
    Async wrapper for the pg-iam database,
    provide async helper methods for calling database functions,
    and executing arbitrary SQL queries.

    Tables
    ------
    persons
    users
    groups
    group_memberships
    group_moderators
    capabilities_http
    capabilities_http_instances
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
    capability_grant_rank_set
    capability_grant_delete
    capability_instance_get
    capabilities_http_sync
    capabilities_http_grants_sync
    capabilities_http_grants_group_add
    capabilities_http_grants_group_remove

    Example usage
    -------------

    from iam.async_pgiam import AsyncDb, async_session_scope, async_iam_engine

    dsn = f'' # some credentials
    engine = async_iam_engine(dsn)
    db = AsyncDb(engine)

    # use raw sql and helper functions
    query = 'select person_id from persons where name=:name'
    pid = (await db.exec_sql(query, {'name': 'Catullus'}))[0][0]
    pgrps = await db.person_groups(pid)
    query = 'select user_name from users where person_id=:pid'
    user_name = (await db.exec_sql(query, {'pid': pid}))[0][0]
    ugrps = await db.user_groups(user_name)
    await db.group_member_add('admin', user_name)
    vals = {'g': 'g1', 'm': 'g2'}
    await db.exec_sql('insert into group_moderators values (:g, :m)', vals, fetch=False)

    # use session for multiple operations
    identity = 'random_person'
    async with async_session_scope(engine, identity) as session:
        result = await db.exec_sql('select * from persons', session=session)

    # Clean up
    await engine.dispose()

    """

    def __init__(self, engine: sqlalchemy.ext.asyncio.AsyncEngine, config: dict = {}) -> None:
        super(AsyncDb, self).__init__()
        if not engine:
            engine = async_iam_engine(dsn_from_config(config))
        self.engine = engine

    async def exec_sql(
        self,
        sql: str,
        params: dict = {},
        fetch: bool = True,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
        as_dicts: bool = False,
    ) -> Union[bool, list]:
        """
        Execute a parameterised SQL query as a prepared statement,
        fetching all results.

        Parameters
        ----------
        sql: str
        params: dict
        fetch: bool, set to False for insert, update and delete
        session_identity: the identity to record in audit
        session: SQLAlchemy async session object
        as_dicts: format data as dictionaries instead of tuples

        Examples
        --------
        await exec_sql('select * from persons where name=:name', {'name': 'Frank'})
        await exec_sql('select * from users')
        await exec_sql('insert into mytable values (:y)', {'y': 5}, fetch=False)

        Returns
        -------
        list of tuples or boolean

        """
        res, out = True, None
        if session:
            result = await session.execute(sqlalchemy.text(sql), params)
            columns = list(result.keys()) if fetch and hasattr(result, 'keys') else None
            if fetch:
                res = result.fetchall()
        else:
            async with async_session_scope(self.engine, session_identity) as session:
                result = await session.execute(sqlalchemy.text(sql), params)
                columns = list(result.keys()) if fetch and hasattr(result, 'keys') else None
                if fetch:
                    res = result.fetchall()

        if fetch:
            out = res
        if as_dicts and fetch:
            out = []
            for row in res:
                record = {}
                for k, v in zip(columns, row):
                    record[k] = v
                out.append(record)
        return out

    async def person_groups(
        self,
        person_id: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
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
        return (
            await self.exec_sql(
                QUERY_PERSON_GROUPS.format(person_id),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def person_capabilities(
        self,
        person_id: str,
        grants=True,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
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
        g = "t" if grants else "f"
        return (
            await self.exec_sql(
                QUERY_PERSON_CAPABILITIES.format(person_id, g),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def person_access(
        self,
        person_id: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
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
        return (
            await self.exec_sql(
                QUERY_PERSON_ACCESS.format(person_id),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def user_groups(
        self,
        user_name,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Get the group memberships for a user.

        Parameters
        ----------
        user_name: str

        Returns
        -------
        dict

        """
        return (
            await self.exec_sql(
                QUERY_USER_GROUPS.format(user_name),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def user_moderators(
        self,
        user_name,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Get the groups which the user moderates.

        Parameters
        ----------
        user_name: str

        Returns
        -------
        dict

        """
        return (
            await self.exec_sql(
                QUERY_USER_MODERATORS.format(user_name),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def user_capabilities(
        self,
        user_name,
        grants: bool = True,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
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
        g = "t" if grants else "f"
        return (
            await self.exec_sql(
                QUERY_USER_CAPABILITIES.format(user_name, g),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def group_members(
        self,
        group_name: str,
        filter_memberships: Optional[bool] = False,
        client_timestamp: Optional[str] = None,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Get the membership graph of group_name.

        Parameters
        ----------
        group_name: str

        Returns
        -------
        dict

        """
        args = f"'{group_name}'"
        if filter_memberships or client_timestamp:
            args = f"{args}, true"
        if client_timestamp:
            args = f"{args}, '{client_timestamp}'"
        return (
            await self.exec_sql(
                QUERY_GROUP_MEMBERS.format(args),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def group_moderators(
        self,
        group_name: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Get the moderators for a group.

        Parameters
        ----------
        group_name: str

        Returns
        -------
        dict

        """
        return (
            await self.exec_sql(
                QUERY_GROUP_MODERATORS.format(group_name),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def group_member_add(
        self,
        group_name: str,
        member: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        weekdays: Optional[dict] = None,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
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
        start_date = f"'{start_date}'" if start_date else "null"
        end_date = f"'{end_date}'" if end_date else "null"
        weekdays = f"'{json.dumps(weekdays)}'" if weekdays else "null"
        return (
            await self.exec_sql(
                QUERY_GROUP_MEMBER_ADD.format(
                    group_name,
                    member,
                    start_date,
                    end_date,
                    weekdays,
                ),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def group_member_remove(
        self,
        group_name: str,
        member: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
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
        return (
            await self.exec_sql(
                QUERY_GROUP_MEMBER_REMOVE.format(group_name, member),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def group_capabilities(
        self,
        group_name,
        grants=True,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
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
        g = "t" if grants else "f"
        return (
            await self.exec_sql(
                QUERY_GROUP_CAPABILITIES.format(group_name, g),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def institution_group_add(
        self,
        institution: str,
        group_name: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Affiliate a group to an institution. An institution can be
        identified by either:

        1) institution_name
        2) institution_group

        Parameters
        ----------
        institution: str, the institution to which the group should be
            affiliated
        group_name: str, the new affiliated group

        Returns
        -------
        dict

        """
        return (
            await self.exec_sql(
                QUERY_INSTITUTION_GROUP_ADD.format(institution, group_name),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def institution_group_remove(
        self,
        institution: str,
        group_name: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Remove affilitation between a group and an institution. A group
        can be identified by either:

        1) person_id
        2) user_name
        3) group (person group, user group, or generic group)

        Parameters
        ----------
        institution: str, the institution from which the group should be
            unaffiliated
        group_name: str, the existing group to unaffiliate

        Returns
        -------
        dict

        """
        return (
            await self.exec_sql(
                QUERY_INSTITUTION_GROUP_REMOVE.format(institution, group_name),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def institution_groups(
        self,
        institution: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Get the affiliation graph of institution.

        Parameters
        ----------
        institution: str

        Returns
        -------
        dict

        """
        return (
            await self.exec_sql(
                QUERY_INSTITUTION_GROUPS.format(institution),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def institution_member_add(
        self,
        institution: str,
        member: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Add a new member to an institution. A new member can be
        identified by either:

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

        Note: internally, pg-iam adds persons to institutions via their
        person group, and users to institutions via their user groups.

        Parameters
        ----------
        institution: str, the institution to which the member should be added
        member: str, the new member

        Returns
        -------
        dict

        """
        return (
            await self.exec_sql(
                QUERY_INSTITUTION_MEMBER_ADD.format(institution, member),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def institution_member_remove(
        self,
        institution: str,
        member: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Remove a member from an institution. A member can be identified
        by either:

        1) person_id
        2) user_name
        3) group (person group, user group, or generic group)

        Parameters
        ----------
        institution: str, the institution from which the member should be
            removed
        group_name: str, the existing member to remove

        Returns
        -------
        dict

        """
        return (
            await self.exec_sql(
                QUERY_INSTITUTION_MEMBER_REMOVE.format(institution, member),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def institution_members(
        self,
        institution: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Get the membership graph of institution.

        Parameters
        ----------
        institution: str

        Returns
        -------
        dict

        """
        return (
            await self.exec_sql(
                QUERY_INSTITUTION_MEMBERS.format(institution),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def project_group_add(
        self,
        project: str,
        group_name: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Affiliate a group to project. A project can be identified
        by either:

        1) project_number
        2) project_group

        Note: internally, pg-iam adds groups to projects via their
        project group.

        Parameters
        ----------
        project: str, the project to which the group should be
            affiliated
        group_name: str, the new affiliated group

        Returns
        -------
        dict

        """
        return (
            await self.exec_sql(
                QUERY_PROJECT_GROUP_ADD.format(project, group_name),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def project_group_remove(
        self,
        project: str,
        group_name: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Remove affilitation between a group and a project. A group
        can be identified by either:

        1) person_id
        2) user_name
        3) group (person group, user group, or generic group)

        Parameters
        ----------
        institution: str, the project from which the group should be
            unaffiliated
        group_name: str, the existing group to unaffiliate

        Returns
        -------
        dict

        """
        return (
            await self.exec_sql(
                QUERY_PROJECT_GROUP_REMOVE.format(project, group_name),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def project_groups(
        self,
        project: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Get the affiliation graph of project.

        Parameters
        ----------
        project: str

        Returns
        -------
        dict

        """
        return (
            await self.exec_sql(
                QUERY_PROJECT_GROUPS.format(project),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def project_institutions(
        self,
        project: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Get the institution graph of project.

        Parameters
        ----------
        project: str

        Returns
        -------
        dict

        """
        return (
            await self.exec_sql(
                QUERY_PROJECT_INSTITUTIONS.format(project),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def capability_grant_rank_set(
        self,
        grant_id: str,
        new_grant_rank: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
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
        return (
            await self.exec_sql(
                QUERY_CAPABILITY_GRANT_RANK_SET.format(grant_id, new_grant_rank),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def capability_grant_delete(
        self,
        grant_id: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Get the resource grants associated with a specific capability.

        Parameters
        ----------
        grant_id: str (uuid4)

        Returns
        -------
        bool

        """
        return (
            await self.exec_sql(
                QUERY_CAPABILITY_GRANT_DELETE.format(grant_id),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def capability_grants_delete(
        self,
        namespace: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> None:
        """
        Delete all grants for the given namespace. The namespace
        can also be a pattern, such as `files%`.

        Parameters
        ----------
        namespace: str

        Returns
        -------
        None

        """
        return await self.exec_sql(
            QUERY_CAPABILITY_GRANTS_DELETE.format(namespace),
            session_identity=session_identity,
            session=session,
            fetch=False,
        )

    async def capability_instance_get(
        self,
        instance_id: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Create a capability instance.

        Parameters
        ----------
        instance_id: str (uuid4)

        Returns
        -------
        dict

        """
        return (
            await self.exec_sql(
                QUERY_CAPABILITY_INSTANCE_GET.format(instance_id),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def capabilities_http_sync(
        self,
        capabilities: list,
        session_identity: Optional[str] = None,
    ) -> dict:
        """
        Synchronise a list of capabilities to the capabilities_http table,
        replacing any existing entries with the same names, adding
        any entries which do not exist, and removing any entries which are
        no longer in the reference data.

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
        await db.capabilities_http_sync(names)

        Returns
        -------
        dict

        """
        incoming_names = []
        for capability in capabilities:
            incoming_names.append(capability.get("capability_name"))
            input_keys = capability.keys()
            for key in CAPABILITIES_REQUIRED_KEYS:
                if key not in input_keys:
                    m = "missing required key: {0} in capability, cannot do sync without error".format(
                        key
                    )
                    raise Exception(m)

        # find existing capabilities
        existing_names = []
        async with async_session_scope(self.engine, session_identity) as session:
            results = await self.exec_sql(QUERY_CAPABILITIES_HTTP_LIST, session=session)
            for result in results:
                existing_names.append(result[0])

        # calculate work to be done
        inserts = set(incoming_names).difference(existing_names)
        updates = set(incoming_names).intersection(existing_names)
        deletes = set(existing_names).difference(incoming_names)

        async with async_session_scope(self.engine, session_identity) as session:
            for capability in capabilities:
                input_keys = capability.keys()
                for column in CAPABILITIES_TABLE_COLUMNS:
                    if column in CAPABILITIES_JSON_COLUMNS and column in input_keys:
                        capability[column] = json.dumps(capability[column])
                    if column not in input_keys:
                        capability[column] = None
                if capability.get("capability_name") in updates:
                    await self.exec_sql(
                        CAPABILITIES_HTTP_UPDATE_QUERY,
                        capability,
                        fetch=False,
                        session=session,
                    )
                elif capability.get("capability_name") in inserts:
                    await self.exec_sql(
                        CAPABILITIES_HTTP_INSERT_QUERY,
                        capability,
                        fetch=False,
                        session=session,
                    )
            if deletes:
                await self.exec_sql(
                    CAPABILITIES_HTTP_DELETE_QUERY,
                    {"deletes": list(deletes)},
                    fetch=False,
                    session=session,
                )

        return {
            "inserts": list(inserts),
            "updates": list(updates),
            "deletes": list(deletes),
        }

    async def capabilities_http_grants_sync(
        self,
        grants: list,
        session_identity: Optional[str] = None,
        static_grants: bool = False,
    ) -> dict:
        """
        Synchronise a list of grants to the capabilities_http_grants table,
        explicitly by capability_grant_name. The caller MUST provide a unique name.
        The caller can optionally provide a UUID for the capability_grant_id
        but it is not strictly necessary. The db will auto-generate one.

        Semantics: over-write or append. The append writes cannot be
        completely atomic, due to how rank numbers are set. When inserting
        a new entry for a given (capability_name, capability_grant_hostname,
        capability_grant_namespace) combination, the default new rank will
        place the entry at the end of the list. If the caller specifies a
        rank that is different from the default value (end of the list)
        then the insert transaction has to be commited before the rank
        can be updated to the desired value. The rank cannot be set in the
        same transation because it requires updating the ranks of other grants.

        So if the call fails for new entries, the caller should just try again,
        since calls are idempotent.

        If static_grants=True, then all grants will be marked as static using the
        capability_grant_static attribute. This will also trigger deletion of grants
        that are no longer referenced in the reference set. Deletions are done per
        namespace, per HTTP method.

        Parameters
        ----------
        grants: list of dicts

        The following dict keys are compulsory:
            capability_grant_name: str
            capability_grant_hostnames: str
            capability_grant_namespace: str
            capability_grant_http_method: str
            capability_grant_rank: int > 0
            capability_grant_uri_pattern: str
            capability_grant_required_groups: list

        Returns
        -------
        dict

        """
        work_done = {
            "inserts": [],
            "updates": [],
            "deletes": [],
        }

        grant_sets = {}
        for grant in grants:
            input_keys = grant.keys()
            for key in GRANTS_REQUIRED_KEYS:
                if key not in input_keys:
                    m = "missing required key: {0} in grant, cannot do sync without error".format(
                        key
                    )
                    raise Exception(m)
            namespace = grant.get("capability_grant_namespace")
            method = grant.get("capability_grant_http_method")
            name = grant.get("capability_grant_name")
            if not grant_sets.get(namespace):
                grant_sets[namespace] = {}
            if not grant_sets.get(namespace).get(method):
                grant_sets[namespace][method] = []
            grant_sets[namespace][method].append(name)

        new_grants = []
        async with async_session_scope(self.engine, session_identity) as session:
            for grant in grants:
                exists = (await self.exec_sql(GRANTS_EXISTS_QUERY, grant, session=session))[
                    0
                ][0]
                input_keys = grant.keys()
                for column in GRANTS_TABLE_COLUMNS:
                    if column in GRANTS_JSON_COLUMNS and column in input_keys:
                        grant[column] = json.dumps(grant[column])
                    if column not in input_keys:
                        if column in [
                            "capability_grant_group_existence_check",
                            "capability_grant_quick",
                        ]:
                            grant[column] = True
                        else:
                            grant[column] = None
                if static_grants:
                    grant["capability_grant_static"] = True
                if exists:
                    await self.exec_sql(
                        GRANTS_UPDATE_QUERY, grant, fetch=False, session=session
                    )
                    curr_grant_id = (
                        await self.exec_sql(
                            GRANTS_GET_ID_FROM_NAME_QUERY,
                            {"name": grant["capability_grant_name"]},
                            session=session,
                        )
                    )[0][0]
                    await self.exec_sql(
                        QUERY_CAPABILITY_GRANT_RANK_SET.format(
                            curr_grant_id, grant["capability_grant_rank"]
                        ),
                        session=session,
                    )
                    work_done["updates"].append(grant.get("capability_grant_name"))
                else:
                    await self.exec_sql(
                        GRANTS_INSERT_QUERY, grant, fetch=False, session=session
                    )
                    curr_grant_id = (
                        await self.exec_sql(
                            GRANTS_GET_ID_FROM_NAME_QUERY,
                            {"name": grant["capability_grant_name"]},
                            session=session,
                        )
                    )[0][0]
                    new_grants.append(
                        {"id": curr_grant_id, "rank": grant["capability_grant_rank"]}
                    )
                    work_done["inserts"].append(grant.get("capability_grant_name"))

        # set the rank values
        async with async_session_scope(self.engine, session_identity) as session:
            for grant in new_grants:
                await self.exec_sql(
                    QUERY_CAPABILITY_GRANT_RANK_SET.format(grant["id"], grant["rank"]),
                    session=session,
                )

        if static_grants:
            for namespace, grant_set in grant_sets.items():
                for method, incoming_names in with_all_http_methods(grant_set).items():
                    existing_names = []
                    async with async_session_scope(self.engine, session_identity) as session:
                        results = await self.exec_sql(
                            GRANTS_FIND_EXISTING_QUERY,
                            {
                                "namespace": namespace,
                                "method": method,
                            },
                            session=session,
                        )
                        for result in results:
                            existing_names.append(result[0])
                        deletes = set(existing_names).difference(incoming_names)
                        if deletes:
                            for name in deletes:
                                grant_id = (
                                    await self.exec_sql(
                                        "select capability_grant_id from capabilities_http_grants where capability_grant_name = :name",
                                        {"name": name},
                                        session=session,
                                    )
                                )[0][0]
                                await self.capability_grant_delete(
                                    grant_id, session_identity, conn
                                )
                                work_done["deletes"].append(name)
        return work_done

    async def capabilities_http_grants_group_add(
        self,
        grant_reference: str,
        group_name: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Add a required group to a grant.

        Parameters
        ----------
        grant_reference: str (capability_grant_id, or capability_grant_name)
        group_name: str

        Returns
        -------
        boolean

        """
        return (
            await self.exec_sql(
                QUERY_CAPABILITY_GRANT_GROUP_ADD.format(grant_reference, group_name),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

    async def capabilities_http_grants_group_remove(
        self,
        grant_reference: str,
        group_name: str,
        session_identity: Optional[str] = None,
        session: Optional[AsyncSession] = None,
    ) -> dict:
        """
        Remove a required group from a grant.

        Parameters
        ----------
        grant_reference: str (capability_grant_id, or capability_grant_name)
        group_name: str

        Returns
        -------
        boolean

        """
        return (
            await self.exec_sql(
                QUERY_CAPABILITY_GRANT_GROUP_REMOVE.format(grant_reference, group_name),
                session_identity=session_identity,
                session=session,
            )
        )[0][0]

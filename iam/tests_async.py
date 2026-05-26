"""
Async tests for pypg-iam AsyncDb class.

Tests both psycopg3 and asyncpg drivers to ensure compatibility.
Requires pytest-asyncio: pip install pytest-asyncio
"""

import os
import pytest
import pytest_asyncio

from .async_pgiam import AsyncDb, async_iam_engine, _detect_available_driver


# Check which drivers are available
AVAILABLE_DRIVERS = []
try:
    import psycopg

    if hasattr(psycopg, "AsyncConnection"):
        AVAILABLE_DRIVERS.append("psycopg")
except ImportError:
    pass

try:
    import asyncpg

    AVAILABLE_DRIVERS.append("asyncpg")
except ImportError:
    pass


@pytest.fixture(params=AVAILABLE_DRIVERS)
def driver(request):
    """Parametrized fixture to run tests with all available async drivers."""
    return request.param


@pytest_asyncio.fixture
async def async_db(driver):
    """
    Create an AsyncDb instance with the specified driver.

    Yields the db instance, then cleans up the engine.
    """
    user = os.environ["PYPGIAM_USER"]
    pw = os.environ["PYPGIAM_PW"]
    host = os.environ["PYPGIAM_HOST"]
    db_name = os.environ["PYPGIAM_DB"]

    dsn = f"postgresql://{user}:{pw}@{host}:5432/{db_name}"
    engine = async_iam_engine(dsn, driver=driver)
    db = AsyncDb(engine)

    yield db

    # Cleanup
    await engine.dispose()


class TestAsyncPgIam:
    """Async tests for pypg-iam that run with both psycopg and asyncpg drivers."""

    async def grant_id_from_name(self, db: AsyncDb, grant_name: str) -> str:
        """Helper to get grant ID from grant name."""
        out = await db.exec_sql(
            "select capability_grant_id from capabilities_http_grants "
            "where capability_grant_name = :gn",
            {"gn": grant_name},
        )
        return str(out[0][0]) if out else None

    async def cleanup(self, db: AsyncDb, pid: str, grants: list, groups: dict) -> None:
        """Helper to cleanup test data."""
        for grant in grants:
            grant_id = await self.grant_id_from_name(db, grant)
            if grant_id:
                await db.capability_grant_delete(grant_id)

        await db.exec_sql(
            "delete from persons where person_id = :pid",
            {"pid": pid},
            fetch=False,
        )
        await db.exec_sql(
            "delete from groups where group_name in (:g1, :g2, :g3, :g4)",
            {
                "g1": groups.get("g1"),
                "g2": groups.get("g2"),
                "g3": groups.get("g3"),
                "g4": groups.get("g4"),
            },
            fetch=False,
        )
        await db.exec_sql(
            "delete from capabilities_http where capability_name in (:n1, :n2, :n3)",
            {"n1": "test1", "n2": "test2", "n3": "test3"},
            fetch=False,
        )

    @pytest.mark.asyncio
    async def test_async_pgiam(self, async_db: AsyncDb, driver: str) -> None:
        """
        Comprehensive async test covering all major AsyncDb operations.

        This test mirrors the sync test_pgiam but uses async/await.
        It runs with both psycopg and asyncpg drivers.
        """
        print(f"\n=== Testing with driver: {driver} ===")

        pid = None

        _in_full_name = "Kor Ah Async"
        _in_uname = "kor1_async"

        _in_group1 = "g1_async"
        _in_group2 = "g2_async"
        _in_group3 = "g3_async"
        _in_group4 = "g4_async"

        groups = {
            "g1": _in_group1,
            "g2": _in_group2,
            "g3": _in_group3,
            "g4": _in_group4,
        }

        grname1 = "grant_1_async"
        grname2 = "grant_2_async"
        grname3 = "grant_3_async"
        grname4 = "grant_4_async"
        grname5 = "grant_5_async"
        grants = [grname1, grname2, grname3, grname4, grname5]

        try:
            # Create a person, get the person ID
            await async_db.exec_sql(
                "insert into persons(full_name) values (:full_name)",
                {"full_name": _in_full_name},
                fetch=False,
            )
            result = await async_db.exec_sql(
                "select person_id from persons where full_name = :full_name",
                {"full_name": _in_full_name},
            )
            pid = result[0][0]

            # Create a user
            await async_db.exec_sql(
                "insert into users(person_id, user_name) values (:pid, :user_name)",
                {"pid": pid, "user_name": _in_uname},
                fetch=False,
            )

            # Create groups
            for _, group in groups.items():
                await async_db.exec_sql(
                    "insert into groups(group_name, group_class, group_type) values (:name, :class, :type)",
                    {"name": group, "class": "secondary", "type": "generic"},
                    fetch=False,
                )

            # Add members
            result1 = await async_db.group_member_add(_in_group1, _in_group2)
            print(f"Add {_in_group2} to {_in_group1}: {result1}")

            result2 = await async_db.group_member_add(_in_group1, _in_group3)
            print(f"Add {_in_group3} to {_in_group1}: {result2}")

            result3 = await async_db.group_member_add(_in_group2, _in_uname)
            print(f"Add {_in_uname} to {_in_group2}: {result3}")

            # Add moderators
            await async_db.exec_sql(
                "insert into group_moderators(group_name, group_moderator_name) values (:group, :mod)",
                {"group": _in_group1, "mod": _in_group4},
                fetch=False,
            )

            # Informational queries
            person_groups = await async_db.person_groups(pid)
            print(f"Person groups: {person_groups}")

            user_groups = await async_db.user_groups(_in_uname)
            print(f"User groups: {user_groups}")

            group_members = await async_db.group_members(_in_group1)
            print(f"Group members: {group_members}")

            group_mods = await async_db.group_moderators(_in_group1)
            print(f"Group moderators: {group_mods}")

            remove_result = await async_db.group_member_remove(_in_group1, _in_group3)
            print(f"Remove {_in_group3} from {_in_group1}: {remove_result}")

            group_members_after = await async_db.group_members(_in_group1)
            print(f"Group members after removal: {group_members_after}")

            # Capabilities
            names1 = [
                {
                    "capability_name": "test1",
                    "capability_required_groups": [_in_group1],
                    "capability_lifetime": 60,
                    "capability_description": "allows testing",
                    "capability_hostnames": [],
                },
                {
                    "capability_name": "test2",
                    "capability_required_groups": [_in_group1],
                    "capability_lifetime": 60,
                    "capability_description": "allows nothing",
                    "capability_hostnames": [],
                },
            ]
            caps_sync1 = await async_db.capabilities_http_sync(names1)
            print(f"Capabilities sync 1: {caps_sync1}")

            caps1 = await async_db.exec_sql(
                "select * from capabilities_http where capability_name in (:n1, :n2)",
                {"n1": "test1", "n2": "test2"},
            )
            print(f"Retrieved capabilities: {len(caps1)} rows")

            # Verify both capabilities exist
            name_col_idx = 2
            group_col_idx = 5
            assert len(caps1) == 2
            assert caps1[0][name_col_idx] == "test1"
            assert len(caps1[0][group_col_idx]) == 1
            assert caps1[0][group_col_idx] == [_in_group1]
            assert caps1[1][name_col_idx] == "test2"
            assert len(caps1[1][group_col_idx]) == 1
            assert caps1[1][group_col_idx] == [_in_group1]

            # Update capabilities
            names2 = [
                {
                    "capability_name": "test1",
                    "capability_required_groups": [_in_group1],
                    "capability_lifetime": 60,
                    "capability_description": "allows one thing",
                    "capability_hostnames": [],
                },
                {
                    "capability_name": "test2",
                    "capability_required_groups": [_in_group2],
                    "capability_lifetime": 60,
                    "capability_description": "allows another thing",
                    "capability_hostnames": [],
                },
                {
                    "capability_name": "test3",
                    "capability_required_groups": [_in_group1, f"{_in_uname}-group"],
                    "capability_lifetime": 60,
                    "capability_description": "allows many things",
                    "capability_hostnames": [],
                },
            ]
            caps_sync2 = await async_db.capabilities_http_sync(names2)
            print(f"Capabilities sync 2: {caps_sync2}")

            caps2 = await async_db.exec_sql(
                "select * from capabilities_http where capability_name in (:n1, :n2, :n3)",
                {"n1": "test1", "n2": "test2", "n3": "test3"},
            )
            assert len(caps2) == 3

            # Grants
            grants1 = [
                {
                    "capability_grant_name": grname1,
                    "capability_names_allowed": ["test1"],
                    "capability_grant_hostnames": ["my.api.com"],
                    "capability_grant_namespace": "files",
                    "capability_grant_http_method": "PUT",
                    "capability_grant_rank": 1,
                    "capability_grant_uri_pattern": "/groups/[a-zA-Z0-9]",
                    "capability_grant_required_groups": ["self", "moderator"],
                    "capability_grant_group_existence_check": False,
                },
                {
                    "capability_grant_name": grname2,
                    "capability_names_allowed": ["test2"],
                    "capability_grant_hostnames": ["my.api.com"],
                    "capability_grant_namespace": "files",
                    "capability_grant_http_method": "HEAD",
                    "capability_grant_rank": 1,
                    "capability_grant_uri_pattern": "/files/export$",
                    "capability_grant_required_groups": [_in_group3, _in_group4],
                    "capability_grant_required_attributes": {
                        "required_claims": ["lol"],
                    },
                },
            ]

            grants_sync1 = await async_db.capabilities_http_grants_sync(
                grants1, static_grants=True
            )
            print(f"Grants sync 1: {grants_sync1}")

            gs1 = await async_db.exec_sql(
                "select * from capabilities_http_grants where capability_grant_name in (:gn1, :gn2)",
                {"gn1": grname1, "gn2": grname2},
            )
            print(f"Retrieved grants: {len(gs1)} rows")

            g_rank_idx = 7
            g_req_gr_idx = 9
            g_req_attr_idx = 10
            assert len(gs1) == 2
            assert gs1[0][g_rank_idx] == 1
            assert gs1[0][g_req_gr_idx] == ["self", "moderator"]
            assert gs1[1][g_rank_idx] == 1
            assert gs1[1][g_req_gr_idx] == [_in_group3, _in_group4]
            assert gs1[1][g_req_attr_idx] == {"required_claims": ["lol"]}

            print(f"All tests passed with {driver} driver!")

        finally:
            # Cleanup
            if pid:
                await self.cleanup(async_db, pid, grants, groups)


@pytest.mark.asyncio
async def test_driver_detection():
    """Test that driver detection works correctly."""
    detected = _detect_available_driver()

    if not AVAILABLE_DRIVERS:
        assert detected is None, "Should detect no drivers when none are installed"
    else:
        assert detected in AVAILABLE_DRIVERS, (
            f"Detected driver {detected} not in available {AVAILABLE_DRIVERS}"
        )
        # psycopg should be prioritized over asyncpg
        if "psycopg" in AVAILABLE_DRIVERS:
            assert detected == "psycopg", (
                "psycopg should be detected first when available"
            )
        else:
            assert detected == "asyncpg", (
                "asyncpg should be detected when psycopg unavailable"
            )


@pytest.mark.asyncio
async def test_explicit_driver_selection():
    """Test that explicit driver selection works for all available drivers."""
    user = os.environ["PYPGIAM_USER"]
    pw = os.environ["PYPGIAM_PW"]
    host = os.environ["PYPGIAM_HOST"]
    db_name = os.environ["PYPGIAM_DB"]

    dsn = f"postgresql://{user}:{pw}@{host}:5432/{db_name}"

    for driver in AVAILABLE_DRIVERS:
        print(f"Testing explicit {driver} selection...")
        engine = async_iam_engine(dsn, driver=driver)

        # Verify the correct dialect is in the URL
        url_str = str(engine.url)
        if driver == "psycopg":
            assert "postgresql+psycopg://" in url_str, (
                f"Expected psycopg dialect in URL: {url_str}"
            )
        elif driver == "asyncpg":
            assert "postgresql+asyncpg://" in url_str, (
                f"Expected asyncpg dialect in URL: {url_str}"
            )

        await engine.dispose()
        print(f"✓ {driver} explicit selection works")


@pytest.mark.asyncio
async def test_invalid_driver():
    """Test that invalid driver names raise ValueError."""
    dsn = "postgresql://user:pw@localhost/db"

    with pytest.raises(ValueError, match="driver must be"):
        async_iam_engine(dsn, driver="invalid")


@pytest.mark.asyncio
@pytest.mark.skipif("psycopg" not in AVAILABLE_DRIVERS, reason="psycopg not installed")
async def test_psycopg_ssl_params():
    """Test that psycopg gets correct SSL parameters."""
    from iam.async_pgiam import _get_connect_args

    args = _get_connect_args("psycopg", require_ssl=True)
    assert args == {"sslmode": "require"}, "psycopg should use sslmode parameter"

    args_no_ssl = _get_connect_args("psycopg", require_ssl=False)
    assert args_no_ssl == {}, "No SSL args when require_ssl=False"


@pytest.mark.asyncio
@pytest.mark.skipif("asyncpg" not in AVAILABLE_DRIVERS, reason="asyncpg not installed")
async def test_asyncpg_ssl_params():
    """Test that asyncpg gets correct SSL parameters."""
    from iam.async_pgiam import _get_connect_args

    args = _get_connect_args("asyncpg", require_ssl=True)
    assert args == {"ssl": "require"}, "asyncpg should use ssl parameter"

    args_no_ssl = _get_connect_args("asyncpg", require_ssl=False)
    assert args_no_ssl == {}, "No SSL args when require_ssl=False"

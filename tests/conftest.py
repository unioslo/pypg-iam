"""
Pytest fixtures for pypg-iam tests.

Sets up a temporary PostgreSQL database with pg-iam schema installed.
Works for both sync and async tests.
"""

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio
from pytest_postgresql import factories
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

from iam import Db, AsyncDb, async_iam_engine


# PostgreSQL process factory - creates a PostgreSQL server instance
postgresql_proc = factories.postgresql_proc(
    port=None,  # Random port
    unixsocketdir=tempfile.gettempdir(),
)

# PostgreSQL database factory - creates a database
postgresql_db = factories.postgresql("postgresql_proc")


@pytest.fixture(scope="session")
def pg_iam_schema_dir():
    """
    Clone pg-iam repository and return path to schema files.

    Clones the repo once per test session and cleans up afterwards.
    """
    tmpdir = tempfile.mkdtemp(prefix="pg-iam-schema-")
    repo_path = Path(tmpdir) / "pg-iam"

    try:
        # Clone pg-iam repository
        subprocess.run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "https://github.com/unioslo/pg-iam.git",
                str(repo_path),
            ],
            check=True,
            capture_output=True,
        )

        schema_dir = repo_path / "src"
        if not schema_dir.exists():
            raise RuntimeError(f"Schema directory not found: {schema_dir}")

        yield schema_dir
    finally:
        # Cleanup
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture(scope="function")
def pgiam_db(postgresql_db, pg_iam_schema_dir, postgresql_proc):
    """
    PostgreSQL database with pg-iam schema installed.

    This fixture:
    1. Creates a fresh PostgreSQL database
    2. Installs pgcrypto extension
    3. Loads pg-iam schema files in the correct order using psql

    Returns the psycopg connection from pytest-postgresql.
    """
    conn = postgresql_db
    cursor = conn.cursor()

    # Create pgcrypto extension
    cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    conn.commit()

    # Get connection info before closing
    dbname = conn.info.dbname

    # Close connection temporarily while we use psql to load schemas
    conn.close()

    # Load schema files in order (from install.sh) using psql
    # This handles metacommands properly
    schema_files = [
        "notifications.sql",
        "audit.sql",
        "identities.sql",
        "capabilities.sql",
        "organisations.sql",
        "clients.sql",
    ]

    # Environment variables for pg-iam install scripts
    env = os.environ.copy()
    env.update(
        {
            "DROP_TABLES": "true",  # Fresh tables for each test
            "DELETE_EXISTING_DATA": "false",
            "KEEP_TEST_DATA": "false",
            "PGHOST": postgresql_proc.host,
            "PGPORT": str(postgresql_proc.port),
            "PGUSER": postgresql_proc.user,
            "PGDATABASE": dbname,
        }
    )

    for schema_file in schema_files:
        schema_path = pg_iam_schema_dir / schema_file
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        # Use psql to execute the SQL file
        result = subprocess.run(
            ["psql", "-1", "-f", str(schema_path)],
            env=env,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to load {schema_file}:\n"
                f"STDOUT: {result.stdout}\n"
                f"STDERR: {result.stderr}"
            )

    # Reconnect
    import psycopg

    conn = psycopg.connect(
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        user=postgresql_proc.user,
        dbname=dbname,
    )

    yield conn

    conn.close()
    # Cleanup is handled by pytest-postgresql


@pytest.fixture(scope="function")
def manual_db_config():
    """
    Check for manual database configuration via environment variables.

    Returns a dict with connection details if all PYPGIAM_* vars are set,
    otherwise returns None (use automated pytest-postgresql).
    """
    required_vars = ["PYPGIAM_USER", "PYPGIAM_HOST", "PYPGIAM_DB"]
    if all(var in os.environ for var in required_vars):
        pw = os.environ.get("PYPGIAM_PW", "")
        return {
            "user": os.environ["PYPGIAM_USER"],
            "password": pw,
            "host": os.environ["PYPGIAM_HOST"],
            "database": os.environ["PYPGIAM_DB"],
            "port": 5432,
        }
    return None


@pytest.fixture(scope="function")
def sync_engine(request, manual_db_config):
    """
    SQLAlchemy sync engine connected to the test database.

    Uses manual database config (PYPGIAM_* env vars) if available,
    otherwise uses the automated postgresql_proc fixture.
    """
    if manual_db_config:
        # Use manual database configuration
        dsn = f"postgresql://{manual_db_config['user']}:{manual_db_config['password']}@{manual_db_config['host']}:{manual_db_config['port']}/{manual_db_config['database']}"
    else:
        # Use automated pytest-postgresql (lazy load to avoid unnecessary setup)
        postgresql_proc = request.getfixturevalue("postgresql_proc")
        pgiam_db = request.getfixturevalue("pgiam_db")
        dsn = f"postgresql://{postgresql_proc.user}@{postgresql_proc.host}:{postgresql_proc.port}/{pgiam_db.info.dbname}"

    engine = create_engine(dsn, poolclass=QueuePool)

    yield engine

    engine.dispose()


@pytest.fixture(scope="function")
def sync_db(sync_engine):
    """
    Db instance for synchronous tests.
    """
    return Db(sync_engine)


# Check which async drivers are available for parametrization
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


@pytest.fixture(params=AVAILABLE_DRIVERS if AVAILABLE_DRIVERS else ["auto"])
def driver(request):
    """
    Parametrized fixture providing driver names.

    Tests using this fixture will run once for each available driver.
    If no drivers are available, uses 'auto' (will fail gracefully).
    """
    return request.param


@pytest_asyncio.fixture(scope="function")
async def async_engine(request, manual_db_config, driver):
    """
    SQLAlchemy async engine connected to the test database.

    Uses manual database config (PYPGIAM_* env vars) if available,
    otherwise uses the automated postgresql_proc fixture.
    Uses the driver from the parametrized driver fixture.
    """
    if manual_db_config:
        # Use manual database configuration
        dsn = f"postgresql://{manual_db_config['user']}:{manual_db_config['password']}@{manual_db_config['host']}:{manual_db_config['port']}/{manual_db_config['database']}"
    else:
        # Use automated pytest-postgresql (lazy load to avoid unnecessary setup)
        postgresql_proc = request.getfixturevalue("postgresql_proc")
        pgiam_db = request.getfixturevalue("pgiam_db")
        dsn = f"postgresql://{postgresql_proc.user}@{postgresql_proc.host}:{postgresql_proc.port}/{pgiam_db.info.dbname}"

    engine = async_iam_engine(dsn, driver=driver)

    yield engine

    await engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def async_db(async_engine):
    """
    AsyncDb instance for asynchronous tests.
    """
    return AsyncDb(async_engine)

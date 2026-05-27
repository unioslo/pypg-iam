
# pypg-iam

Python library for [pg-iam](https://github.com/unioslo/pg-iam).

## Installation

### Basic installation (sync only)

```bash
pip install pypg-iam
# or
poetry add pypg-iam
```

This installs dependencies required for the synchronous API only, which uses
SQLAlchemy and psycopg2.

### Installation with async support

**Note:** Async support requires SQLAlchemy 2.0 or higher. The synchronous API
works with SQLAlchemy 1.4+.

```bash
# Install with all async database drivers: psycopg 3 will be used by default
pip install pypg-iam[async]
# or
poetry add pypg-iam[async]
# or
uv add pypg-iam[async]

# Install with only psycopg 3 (the most widely used PostgreSQL driver for Python, recommended)
pip install pypg-iam[async-psycopg]

# Alternative: only asyncpg (optimized for performance)
pip install pypg-iam[async-asyncpg]
```

This will install SQLAlchemy 2.x with async support and async database libraries
for it to use. It's recommended to [pick a specific database
driver](#choosing-an-async-driver) for your project instead of adding
everything, but the "async" dependency group is provided for convenience.

## Features

* **Synchronous API**: Traditional blocking operations using SQLAlchemy
* **Async API** *(optional)*: Native async/await support using SQLAlchemy's asyncio extension with psycopg 3 or asyncpg for modern async frameworks (FastAPI, aiohttp, etc.)

## Usage

### Synchronous

```python
from iam import Db, iam_engine

dsn = "postgresql://user:pw@host:5432/dbname"
engine = iam_engine(dsn)
db = Db(engine)

# Query data
result = db.exec_sql("select * from persons where name=:name", {'name': 'Alice'})
groups = db.person_groups(person_id)
```

### Asynchronous

```python
import asyncio
from iam import AsyncDb, async_iam_engine

async def main():
    dsn = "postgresql://user:pw@host:5432/dbname"

    # Auto-detect: tries psycopg 3 first, then asyncpg
    engine = async_iam_engine(dsn)

    # Or explicitly request a specific driver:
    # engine = async_iam_engine(dsn, driver='psycopg')  # psycopg 3
    # engine = async_iam_engine(dsn, driver='asyncpg')  # asyncpg

    db = AsyncDb(engine)

    # Query data
    result = await db.exec_sql("select * from persons where name=:name", {'name': 'Alice'})
    groups = await db.person_groups(person_id)

    # Clean up
    await engine.dispose()

asyncio.run(main())
```

#### Choosing an async driver

Both psycopg 3 and asyncpg are supported. Choose based on your needs:

* `pypg-iam[async-psycopg]` - **[psycopg 3.x](https://www.psycopg.org/psycopg3/docs/)** (default)
  * The most widely used PostgreSQL driver, full feature coverage
* `pypg-iam[async-asyncpg]` - **[asyncpg](https://magicstack.github.io/asyncpg/current/)**
  * Faster, optimized for high-throughput applications

When calling `async_iam_engine(dsn, driver='auto')` (`auto` keyword being the
default), pypg-iam will use psycopg 3 if available, otherwise asyncpg. If you
explicitly request a driver that isn't installed, you'll get an ImportError with
installation instructions.

## Tests

Tests use `pytest-postgresql` to automatically set up a temporary PostgreSQL database with pg-iam schemas. No manual database setup required!

### Prerequisites

The test suite automatically:

* Clones the [pg-iam](https://github.com/unioslo/pg-iam) repository to get schema files
* Starts a temporary PostgreSQL server
* Installs pg-iam schemas
* Runs tests
* Cleans up everything

You just need PostgreSQL binaries installed on your system (`psql`, `initdb`, etc.).

### Running tests

```bash
# Install all dependencies including test and async extras
uv sync --all-extras

# Run all tests (sync + async with both psycopg and asyncpg drivers)
uv run pytest -v

# Run only sync tests
uv run pytest tests/test_sync.py -v

# Run only async tests
uv run pytest tests/test_async.py -v

# Run async tests with a specific driver
uv run pytest tests/test_async.py::TestAsyncPgIam::test_async_pgiam[psycopg] -v
uv run pytest tests/test_async.py::TestAsyncPgIam::test_async_pgiam[asyncpg] -v
```

### Testing against an existing database (optional)

The test fixtures automatically detect and use an existing pg-iam database if you set these environment variables:

```bash
# Set postgres environment variables
export PYPGIAM_USER="your_user"
export PYPGIAM_PW="your_password"  # Optional, defaults to empty string
export PYPGIAM_HOST="localhost"
export PYPGIAM_DB="your_db"

# Run tests - they'll use your database instead of creating a temporary one
uv run pytest -v
```

**Note:** When using a manual database, the tests expect pg-iam schemas to already be installed. The automated schema installation only happens with pytest-postgresql's temporary databases.

## LICENSE

BSD.

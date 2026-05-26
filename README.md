
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

## Running tests

### Synchronous tests

```bash
poetry install

# set postgres environment variables for pg-iam db access
export PYPGIAM_USER=""
export PYPGIAM_PW=""
export PYPGIAM_HOST=""
export PYPGIAM_DB=""

# run sync tests
poetry run pytest iam/tests.py
```

### Async tests

Async tests require pytest-asyncio and at least one async driver:

```bash
# Install test dependencies and async driver(s)
poetry install --all-extras

# Run async tests (tests both drivers if both are installed)
poetry run pytest iam/tests_async.py -v
```

## LICENSE

BSD.

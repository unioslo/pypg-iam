from .pgiam import Db, iam_engine, session_scope
from ._util import dsn_from_config

__all__ = [
    "Db",
    "iam_engine",
    "session_scope",
    "dsn_from_config",
]

# Optional async support (requires asyncpg to be installed)
try:
    from .async_pgiam import AsyncDb, async_iam_engine, async_session_scope

    __all__.extend(["AsyncDb", "async_iam_engine", "async_session_scope"])
except ImportError:
    # asyncpg not installed - async support not available
    pass

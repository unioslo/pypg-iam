"""Common utilities shared between sync and async implementations."""


def dsn_from_config(config: dict) -> str:
    """
    Build a PostgreSQL DSN string from a config dictionary.

    Parameters
    ----------
    config: dict with keys 'user', 'pw', 'host', 'dbname'

    Returns
    -------
    str: PostgreSQL connection string
    """
    return f"postgresql://{config['user']}:{config['pw']}@{config['host']}:5432/{config['dbname']}"


def with_all_http_methods(grant_set: dict) -> dict:
    """
    Ensure all HTTP methods are present in the grant set.

    If incoming static grants have removed all grants
    associated with an HTTP method, then we have to
    make sure that we search for that method in the DB
    when identifying which grants to delete.

    Parameters
    ----------
    grant_set: dict mapping HTTP methods to lists of grant names

    Returns
    -------
    dict: grant_set with all HTTP methods present
    """
    methods = ["OPTIONS", "GET", "PUT", "POST", "PATCH", "DELETE"]
    for method in methods:
        if method not in grant_set:
            grant_set[method] = []
    return grant_set

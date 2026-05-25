"""Shared MySQL connection helpers (no hook imports)."""


def connection_set_autocommit(conn, enabled: bool) -> None:
    """Support mysqlclient (callable), PyMySQL (callable), and mysql-connector (property)."""
    ac = getattr(conn, "autocommit", None)
    if callable(ac):
        try:
            ac(enabled)
        except TypeError:
            conn.autocommit = enabled
    else:
        try:
            conn.autocommit = enabled
        except (AttributeError, TypeError):
            pass

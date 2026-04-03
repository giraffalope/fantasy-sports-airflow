"""MySQL hook for NASCAR DAGs: align session with ``nascar`` DDL (utf8mb4 / utf8mb4_uca1400_ai_ci).

Airflow's :class:`~airflow.providers.mysql.hooks.mysql.MySqlHook` does not pass ``collation`` or
``init_command`` from connection ``extra``. mysqlclient accepts ``collation=…`` on connect (it
calls ``set_character_set``); mysql-connector-python accepts ``charset`` / ``collation`` as well.

Put ``charset`` and ``collation`` on the URI query (see docker-compose) so the driver sets session
collation without a separate ``SET NAMES``. If neither ``collation`` nor ``init_command`` is in
``extra``, we fall back to ``init_command`` for older drivers / minimal configs.
"""

from __future__ import annotations

from airflow.providers.mysql.hooks.mysql import MySqlHook

_DEFAULT_CHARSET = "utf8mb4"
_DEFAULT_COLLATION = "utf8mb4_uca1400_ai_ci"
_DEFAULT_INIT_COMMAND = f"SET NAMES {_DEFAULT_CHARSET} COLLATE {_DEFAULT_COLLATION}"


class NascarMySqlHook(MySqlHook):
    def _get_conn_config_mysql_client(self, conn):
        cfg = super()._get_conn_config_mysql_client(conn)
        charset = cfg.get("charset") or conn.extra_dejson.get("charset") or _DEFAULT_CHARSET
        cfg["charset"] = charset
        if str(charset).lower() in ("utf8", "utf-8"):
            cfg["use_unicode"] = True
        collation = conn.extra_dejson.get("collation")
        if collation:
            cfg["collation"] = str(collation)
        init_cmd = self.init_command or conn.extra_dejson.get("init_command")
        if init_cmd:
            cfg["init_command"] = init_cmd
        elif not collation:
            cfg["init_command"] = _DEFAULT_INIT_COMMAND
        return cfg

    def _get_conn_config_mysql_connector_python(self, conn):
        cfg = super()._get_conn_config_mysql_connector_python(conn)
        charset = conn.extra_dejson.get("charset") or _DEFAULT_CHARSET
        cfg["charset"] = charset
        collation = conn.extra_dejson.get("collation")
        if collation:
            cfg["collation"] = str(collation)
        init_cmd = self.init_command or conn.extra_dejson.get("init_command")
        if init_cmd:
            cfg["init_command"] = init_cmd
        elif not collation:
            cfg["init_command"] = _DEFAULT_INIT_COMMAND
        return cfg

import logging
import os

from jinja2 import Template

from airflow_utils.nascar_mysql_hook import NascarMySqlHook


def _connection_set_autocommit(conn, enabled: bool) -> None:
    """Support mysqlclient (callable), PyMySQL (callable), and mysql-connector (property)."""
    ac = getattr(conn, "autocommit", None)
    if callable(ac):
        try:
            ac(enabled)
        except TypeError:
            conn.autocommit = enabled  # type: ignore[misc, assignment]
    else:
        try:
            conn.autocommit = enabled  # type: ignore[misc, assignment]
        except (AttributeError, TypeError):
            pass


def execute_sql_statements_transactional(hook: NascarMySqlHook, statements: list[str]) -> None:
    """
    Run all statements on a single connection: commit if every statement succeeds, otherwise rollback.

    MySQL issues an implicit commit before and after DDL (e.g. ``CREATE TABLE``, ``DROP TABLE``), so
    a script that mixes DDL with DML cannot be rolled back to its pre-file state as a whole. Any
    DML that runs after the last DDL in a failed file may still be rolled back; work committed by
    implicit DDL (including an early ``DELETE`` followed by ``CREATE TABLE``) is not undone.
    """
    if not statements:
        return

    conn = hook.get_conn()
    try:
        _connection_set_autocommit(conn, False)
        cursor = conn.cursor()
        try:
            for stmt in statements:
                cursor.execute(stmt)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    finally:
        try:
            _connection_set_autocommit(conn, True)
        except Exception:
            pass
        conn.close()


class MySqlTemplateFileExecutor(NascarMySqlHook):
    def run_templated_sql_file(self, file_path, context=None):
        """
        Executes a .sql Jinja template file with multiple SQL statements separated by '---'
        """
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"SQL file not found: {file_path}")

        logging.info(f"Attempting to read {file_path}")
        with open(file_path) as f:
            raw_template = f.read()

        # Render Jinja template
        rendered_sql = Template(raw_template).render(context or {})

        # Split on triple dash separator
        statements = [stmt.strip() for stmt in rendered_sql.split("---") if stmt.strip()]

        execute_sql_statements_transactional(self, statements)

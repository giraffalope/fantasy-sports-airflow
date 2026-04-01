import os
from collections.abc import Callable
from typing import Any

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sdk import BaseOperator


class MySQLTemplateFileOperator(BaseOperator):
    """
    Executes SQL statements from a Jinja-templated file using MySQL.

    :param sql_file_path: Path to your .sql file
    :param mysql_conn_id: Airflow connection ID
    :param parameters: Dict of Jinja parameters
    :param multi: Whether to execute statements as multi=True
    """

    # sql_file_path is intentionally not templated: Jinja rendering resolves Path objects and
    # raises FileNotFoundError when parent directories (e.g. …/sql/) are absent in the image.
    template_fields = ("parameters",)

    def __init__(
        self,
        *,
        sql_file_path,
        mysql_conn_id="mysql_default",
        parameters=None,
        multi=True,
        **kwargs,
    ):
        # Store as ``str``: Airflow 3 template rendering resolves ``pathlib.Path`` (calling
        # ``resolve()``), which fails when parent dirs are missing in the worker image even if
        # the DAG parse host had the tree mounted differently.
        sql_file_path = os.fspath(sql_file_path)
        super().__init__(**kwargs)
        self.sql_file_path = sql_file_path
        self.mysql_conn_id = mysql_conn_id
        self.parameters = parameters or {}
        self.multi = multi

    def execute(self, context):
        # Read SQL template file
        with open(self.sql_file_path) as f:
            sql_template = f.read()

        # Render with Airflow context + user params
        rendered_sql = self.render_template(sql_template, {**context, **self.parameters})

        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)

        conn = hook.get_conn()
        cursor = conn.cursor()

        statements = rendered_sql.strip().split("---")
        for stmt in statements:
            stmt = stmt.strip()
            if stmt:
                self.log.info("Executing statement:\n%s", stmt)
                cursor.execute(stmt)

        conn.commit()
        cursor.close()
        conn.close()


class MySQLTemplateFileOperatorFromContext(MySQLTemplateFileOperator):
    """
    Builds ``parameters`` inside ``execute()`` from the real task context dict.

    Airflow only Jinja-templates ``MySQLTemplateFileOperator.parameters`` using a reduced
    variable set. Asset-triggered runs often omit top-level ``logical_date`` and
    ``data_interval_start``, so use this class and resolve paths / timestamps in Python
    (e.g. from ``dag_run.run_id``, ``triggering_asset_events``, etc.).
    """

    template_fields = ()

    def __init__(
        self,
        *,
        sql_file_path,
        parameters_factory: Callable[[dict[str, Any]], dict[str, Any]],
        mysql_conn_id="mysql_default",
        multi=True,
        **kwargs,
    ):
        super().__init__(
            sql_file_path=sql_file_path,
            mysql_conn_id=mysql_conn_id,
            parameters={},
            multi=multi,
            **kwargs,
        )
        self.parameters_factory = parameters_factory

    def execute(self, context):
        self.parameters = self.parameters_factory(context)
        return super().execute(context)

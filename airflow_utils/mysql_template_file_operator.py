import os

from airflow.sdk import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

class MySQLTemplateFileOperator(BaseOperator):
    """
    Executes SQL statements from a Jinja-templated file using MySQL.
    
    :param sql_file_path: Path to your .sql file
    :param mysql_conn_id: Airflow connection ID
    :param parameters: Dict of Jinja parameters
    :param multi: Whether to execute statements as multi=True
    """

    template_fields = ("sql_file_path", "parameters")

    def __init__(
        self,
        *,
        sql_file_path,
        mysql_conn_id="mysql_default",
        parameters=None,
        multi=True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql_file_path = sql_file_path
        self.mysql_conn_id = mysql_conn_id
        self.parameters = parameters or {}
        self.multi = multi

    def execute(self, context):
        # Read SQL template file
        with open(self.sql_file_path, "r") as f:
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

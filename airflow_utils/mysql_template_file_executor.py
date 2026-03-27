import os
import logging
from airflow.providers.mysql.hooks.mysql import MySqlHook
from jinja2 import Template

class MySqlTemplateFileExecutor(MySqlHook):
    def run_templated_sql_file(self, file_path, context=None):
        """
        Executes a .sql Jinja template file with multiple SQL statements separated by '---'
        """
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"SQL file not found: {file_path}")

        logging.info(f"Attempting to read {file_path}")
        with open(file_path, 'r') as f:
            raw_template = f.read()

        # Render Jinja template
        rendered_sql = Template(raw_template).render(context or {})

        # Split on triple dash separator
        statements = [stmt.strip() for stmt in rendered_sql.split('---') if stmt.strip()]

        # Execute each statement
        for statement in statements:
            self.run(statement)

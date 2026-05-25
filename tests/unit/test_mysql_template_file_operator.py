"""Unit tests for MySQL template file operators."""

from __future__ import annotations

from unittest.mock import MagicMock, mock_open, patch

from airflow_utils.mysql_template_file_operator import (
    MySQLTemplateFileOperator,
    MySQLTemplateFileOperatorFromContext,
)


def test_mysql_template_file_operator_splits_on_separator() -> None:
    sql = "DROP TABLE foo;\n---\nINSERT INTO bar VALUES (1);"
    op = MySQLTemplateFileOperator(
        task_id="t1",
        sql_file_path="/tmp/test.sql",
        mysql_conn_id="nascar_conn",
        parameters={"series_id": 1},
    )
    op.render_template = MagicMock(return_value=sql)

    with (
        patch("builtins.open", mock_open(read_data=sql)),
        patch(
            "airflow_utils.mysql_template_file_operator.execute_sql_statements_transactional"
        ) as mock_exec,
        patch(
            "airflow_utils.mysql_template_file_operator.NascarMySqlHook",
            return_value=MagicMock(),
        ),
    ):
        op.execute({"dag": object()})

    statements = mock_exec.call_args.args[1]
    assert statements == ["DROP TABLE foo;", "INSERT INTO bar VALUES (1);"]


def test_mysql_template_file_operator_from_context_uses_parameters_factory() -> None:
    sql = "CALL nascar.proc({{ series_id }});"
    factory = MagicMock(return_value={"series_id": 99})
    op = MySQLTemplateFileOperatorFromContext(
        task_id="t2",
        sql_file_path="/tmp/ctx.sql",
        mysql_conn_id="nascar_conn",
        parameters_factory=factory,
    )
    op.render_template = MagicMock(return_value=sql)
    context = {"logical_date": "2026-01-01"}

    with (
        patch("builtins.open", mock_open(read_data=sql)),
        patch("airflow_utils.mysql_template_file_operator.execute_sql_statements_transactional"),
        patch(
            "airflow_utils.mysql_template_file_operator.NascarMySqlHook",
            return_value=MagicMock(),
        ),
    ):
        op.execute(context)

    factory.assert_called_once_with(context)
    assert op.parameters == {"series_id": 99}

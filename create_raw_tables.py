from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from snowflake.snowpark import Session


# Define the Snowflake connection parameters as Airflow variables.
# This makes it easier to reuse them across DAGs and to manage them via the UI.
snowflake_conn_params = {
    "account": Variable.get("snowflake_account"),
    "user": Variable.get("snowflake_user"),
    "password": Variable.get("snowflake_password"),
    "schema": Variable.get("snowflake_schema"),
    "warehouse": Variable.get("snowflake_warehouse"),
    "database": Variable.get("snowflake_database"),
    "role": Variable.get("snowflake_role"),
}


def create_table(sql_file_path):
    # Read SQL file contents
    with open(sql_file_path, "r") as sql_file:
        sql_contents = sql_file.read()

    with Session.builder.configs(snowflake_conn_params).create() as s:
        result = s.sql(sql_contents).collect()
    return result


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 4, 25),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "create_tables_dag",
    default_args=default_args,
    description="Create tables in Snowflake",
    schedule_interval=None,
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="create_application_record_task",
        python_callable=create_table,
        op_kwargs={"sql_file_path": "sql/application_record_table.sql "},
    )

    t2 = PythonOperator(
        task_id="create_credit_task",
        python_callable=create_table,
        op_kwargs={"sql_file_path": "sql/credit_record_table.sql "},
    )


[t1, t2]

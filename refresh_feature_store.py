from datetime import datetime, timedelta


from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from snowflake.snowpark import Session


snowflake_conn_params = {
    "account": Variable.get("snowflake_account"),
    "user": Variable.get("snowflake_user"),
    "password": Variable.get("snowflake_password"),
    "schema": Variable.get("snowflake_schema"),
    "warehouse": Variable.get("snowflake_warehouse"),
    "database": Variable.get("snowflake_database"),
    "role": Variable.get("snowflake_role"),
}


def create_label():
    with Session.builder.configs(snowflake_conn_params).create() as s:
        result = s.call("create_label")
    return result


def create_ml_raw_data():
    with Session.builder.configs(snowflake_conn_params).create() as s:
        result = s.call("create_ml_raw_data")
    return result


def create_test_train_data():
    with Session.builder.configs(snowflake_conn_params).create() as s:
        result = s.call("create_test_train_data")
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
    "train",
    default_args=default_args,
    description="train ml model",
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="create_label", python_callable=create_label)
    t2 = PythonOperator(
        task_id="create_ml_raw_data", python_callable=create_ml_raw_data
    )
    t3 = PythonOperator(
        task_id="create_test_train_data", python_callable=create_test_train_data
    )

t1 >> t2 >> t3

from datetime import datetime, timedelta
import os
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


def find_model_id():
    with Session.builder.configs(snowflake_conn_params).create() as s:
        model_id = s.sql(
            f"select model_id from model_performance order by model score desc limit 1"
        ).collect()
        s.file.get(f"@model_stage/{model_id}.pkl", "model")
        os.rename(f"model/{model_id}.pkl", "model/model.pkl")
        s.file.put(
            "model/model.pkl", "@prod_model_stage", auto_compress=False, overwrite=True
        )
    return


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 4, 25),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "find_best_model",
    default_args=default_args,
    description="find and promote best model",
    schedule_interval=None,
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="find_model_id",
        python_callable=find_model_id,
    )
t1

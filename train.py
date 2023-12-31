import os
import pickle
import sys
from datetime import datetime, timedelta

import cachetools
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import OneHotEncoder
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark.functions import max as max_
from snowflake.snowpark.functions import sproc, udf, when
from xgboost import XGBClassifier

snowflake_conn_params = {
    "account": Variable.get("snowflake_account"),
    "user": Variable.get("snowflake_user"),
    "password": Variable.get("snowflake_password"),
    "schema": Variable.get("snowflake_schema"),
    "warehouse": Variable.get("snowflake_warehouse"),
    "database": Variable.get("snowflake_database"),
    "role": Variable.get("snowflake_role"),
}


def train_model():
    with Session.builder.configs(snowflake_conn_params).create() as s:
        result = s.call("train_model")
    return result


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
    "train",
    default_args=default_args,
    description="train ml model",
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    t4 = PythonOperator(task_id="train_model", python_callable=train_model)
    t5 = PythonOperator(task_id="register_predict", python_callable=register_predict)

t4 >> t5

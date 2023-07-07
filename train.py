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


def train_model():
    with Session.builder.configs(snowflake_conn_params).create() as s:
        result = s.call("train_model")
    return result


def register_predict():
    with Session.builder.configs(snowflake_conn_params).create() as s:
        s.add_import("@models_stage/model.pkl")
        s.add_packages(
            "snowflake-snowpark-python",
            "scikit-learn",
            "pandas",
            "cachetools",
            "xgboost",
        )

        @udf(
            name="predict_credit",
            is_permanent=True,
            stage_location="@credit_score_stage",
            replace=True,
        )
        def predict(
            ID: int,
            CODE_GENDER: str,
            FLAG_OWN_CAR: str,
            FLAG_OWN_REALTY: str,
            CNT_CHILDREN: int,
            AMT_INCOME_TOTAL: float,
            NAME_INCOME_TYPE: str,
            NAME_EDUCATION_TYPE: str,
            NAME_FAMILY_STATUS: str,
            NAME_HOUSING_TYPE: str,
            DAYS_BIRTH: int,
            DAYS_EMPLOYED: int,
            FLAG_MOBIL: int,
            FLAG_WORK_PHONE: int,
            FLAG_PHONE: int,
            FLAG_EMAIL: int,
            OCCUPATION_TYPE: str,
            CNT_FAM_MEMBERS: int,
        ) -> float:
            @cachetools.cached(cache={})
            def read_file(file_name):
                import pickle

                import_dir = sys._xoptions.get("snowflake_import_directory")
                if import_dir:
                    with open(os.path.join(import_dir, file_name), "rb") as f:
                        model = pickle.load(f)
                    return model

            import pandas as pd

            features = [
                "ID",
                "CODE_GENDER",
                "FLAG_OWN_CAR",
                "FLAG_OWN_REALTY",
                "CNT_CHILDREN",
                "AMT_INCOME_TOTAL",
                "NAME_INCOME_TYPE",
                "NAME_EDUCATION_TYPE",
                "NAME_FAMILY_STATUS",
                "NAME_HOUSING_TYPE",
                "DAYS_BIRTH",
                "DAYS_EMPLOYED",
                "FLAG_MOBIL",
                "FLAG_WORK_PHONE",
                "FLAG_PHONE",
                "FLAG_EMAIL",
                "OCCUPATION_TYPE",
                "CNT_FAM_MEMBERS",
            ]
            model = read_file("model.pkl")
            row = pd.DataFrame([locals()], columns=features)
            return model.predict(row)[0]


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
    t4 = PythonOperator(task_id="train_model", python_callable=train_model)
    t5 = PythonOperator(task_id="register_predict", python_callable=register_predict)

t4 >> t5

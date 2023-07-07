from datetime import datetime, timedelta
import cachetools
import sys

import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.functions import col, udf


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
            f"select model_id from model_performance order by model_score desc limit 1"
        ).collect()
        model_id = str(model_id).split("'")[1]
        s.file.get(f"@model_stage/{model_id}.pkl", "model")
        os.rename(f"model/{model_id}.pkl", "model.pkl")
        s.file.put(
            "model.pkl", "@prod_model_stage", auto_compress=False, overwrite=True
        )
    return None


def bulk_predict():
    with Session.builder.configs(snowflake_conn_params).create() as s:
        s.add_import("@prod_model_stage/model.pkl")
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
            stage_location="@sproc_stage",
            replace=True,
        )
        def predict_credit(
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

        test_df = s.table("test_data")
        x_df = test_df.drop("label")
        results = test_df.select(
            col("id"), predict_credit(*x_df).alias("prediction"), col("label")
        )
        results.write.mode("overwrite").save_as_table("prediction_results")
    return None


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

    t2 = PythonOperator(
        task_id="register_best_model",
        python_callable=bulk_predict,
    )
t1 >> t2

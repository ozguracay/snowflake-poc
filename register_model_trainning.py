import io
import pickle
from datetime import datetime, timedelta
import uuid
import json


from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import OneHotEncoder
from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc
from xgboost import XGBClassifier
from snowflake.snowpark.types import StringType, TimeType, VariantType, FloatType


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


def register_model_training():
    with Session.builder.configs(snowflake_conn_params).create() as s:
        s.add_packages(
            "snowflake-snowpark-python",
            "scikit-learn",
            "pandas",
            "cachetools",
            "xgboost",
        )

        @sproc(
            name="train_model",
            replace=True,
            is_permanent=True,
            stage_location="@sproc_stage",
        )
        def train_model(session: Session) -> None:
            hyper_parameters = {
                "max_depth": 4,
                "eta": 0.2,
            }

            model_id = uuid.uuid4()
            df = session.table("TRAIN_DATA").to_pandas()
            df_test = session.table("TEST_DATA").to_pandas()

            x = df[df.columns[:-1]]
            y = df["LABEL"]

            x_test = df_test[df_test.columns[:-1]]
            y_test = df_test["LABEL"]

            categorical_columns = x.select_dtypes(include=["object"]).columns.to_list()

            categorical_pipeline = Pipeline(
                [("OneHot", OneHotEncoder(handle_unknown="ignore"))]
            )
            transformer = ColumnTransformer(
                [("cat", categorical_pipeline, categorical_columns)]
            )
            xgb = XGBClassifier(**hyper_parameters)

            pipeline = make_pipeline(transformer, xgb)
            pipeline.fit(x, y)
            test_score = pipeline.score(x_test, y_test)

            input_stream = io.BytesIO()
            pickle.dump(pipeline, input_stream)
            session._conn._cursor.upload_stream(
                input_stream, f"@model_stage/{model_id}.pkl"
            )

            df = session.create_dataframe(
                [
                    (
                        f"{model_id}",
                        json.dumps(hyper_parameters),
                        datetime.now(),
                        float(test_score),
                    )
                ],
                schema=["MODEL_ID", "HYPER_PARAMETERS", "TRAINING_TIME", "MODEL_SCORE"],
            )

            df.write.mode("append").save_as_table("MODEL_PERFORMANCE")
            return None


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
    "register_training_pipeline",
    default_args=default_args,
    description="register model training pipeline",
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    t5 = PythonOperator(
        task_id="create_model_training", python_callable=register_model_training
    )

t5

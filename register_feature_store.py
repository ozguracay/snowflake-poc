from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark.functions import max as max_
from snowflake.snowpark.functions import sproc, when

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


def register_label_stage():
    with Session.builder.configs(snowflake_conn_params).create() as s:
        s.add_packages("snowflake-snowpark-python")

        @sproc(
            name="create_label",
            replace=True,
            is_permanent=True,
            stage_location="@sproc_stage",
        )
        def create_credit_score_label(session: Session) -> None:
            df = (
                session.table("CREDIT_RECORDS")
                .select(
                    col("ID"),
                    when(
                        col("STATUS").isin(["0", "1", "2", "3", "4", "5"]),
                        lit(1),
                    )
                    .otherwise(lit(0))
                    .alias("LABEL"),
                )
                .groupBy(col("ID"))
                .agg(max_("LABEL").alias("LABEL"))
            )

            df.write.mode("overwrite").save_as_table("CREDIT_RECORDS_WITH_LABEL")

        return None


def register_ml_raw_data():
    with Session.builder.configs(snowflake_conn_params).create() as s:
        s.add_packages("snowflake-snowpark-python")

        @sproc(
            name="create_ml_raw_data",
            replace=True,
            is_permanent=True,
            stage_location="@sproc_stage",
        )
        def combine_label_and_features(session: Session) -> None:
            df_application = session.table("APPLICATION_RECORDS")
            df_label = session.table("CREDIT_RECORDS_WITH_LABEL")

            df_application.join(
                df_label,
                on=(df_application.ID == df_label.ID),
                how="left",
                rsuffix="rs",
            ).drop("IDRS").filter(col("LABEL").isNotNull()).write.mode(
                "overwrite"
            ).save_as_table(
                "ML_RAW_DATA"
            )
            return None


def register_test_train_data():
    with Session.builder.configs(snowflake_conn_params).create() as s:
        s.add_packages("snowflake-snowpark-python")

        @sproc(
            name="create_test_train_data",
            replace=True,
            is_permanent=True,
            stage_location="@sproc_stage",
        )
        def test_train_split(session: Session) -> None:
            df = session.table("ML_RAW_DATA")
            df_train, df_test = df.random_split([0.8, 0.2], seed=42)

            df_train.write.mode("overwrite").save_as_table("TRAIN_DATA")
            df_test.write.mode("overwrite").save_as_table("TEST_DATA")
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
    "register_feature_store_sprocs",
    default_args=default_args,
    description="register all stages, tables and store procedures",
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="create_label", python_callable=register_label_stage)
    t2 = PythonOperator(
        task_id="create_raw_ml_data", python_callable=register_ml_raw_data
    )
    t3 = PythonOperator(
        task_id="create_test_train_data", python_callable=register_test_train_data
    )


[t1, t2, t3]

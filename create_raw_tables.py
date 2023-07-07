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

app_sql = f"""
create or replace table application_records(
    ID int,
    CODE_GENDER varchar,
    FLAG_OWN_CAR varchar,
    FLAG_OWN_REALTY varchar,
    CNT_CHILDREN number,
    AMT_INCOME_TOTAL double,
    NAME_INCOME_TYPE varchar,
    NAME_EDUCATION_TYPE varchar,
    NAME_FAMILY_STATUS varchar,
    NAME_HOUSING_TYPE varchar,
    DAYS_BIRTH number,
    DAYS_EMPLOYED number,
    FLAG_MOBIL int,
    FLAG_WORK_PHONE int,
    FLAG_PHONE int,
    FLAG_EMAIL int,
    OCCUPATION_TYPE varchar,
    CNT_FAM_MEMBERS number
);

"""
cre_sql = f"""
create or replace table credit_records(
    id int,
    months_balance number,
    status varchar
); 

"""


def run_sql(sql_contents):
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
        task_id="create_database",
        python_callable=run_sql,
        op_kwargs={"sql_contents": "create database if not exists staples;"},
    )
    t2 = PythonOperator(
        task_id="create_schema",
        python_callable=run_sql,
        op_kwargs={"sql_contents": "create schema if not exists credit_score;"},
    )

    t3 = PythonOperator(
        task_id="create_application_record_task",
        python_callable=run_sql,
        op_kwargs={"sql_contents": app_sql},
    )

    t4 = PythonOperator(
        task_id="create_credit_task",
        python_callable=run_sql,
        op_kwargs={"sql_contents": cre_sql},
    )


t1 >> t2 >> [t3, t4]

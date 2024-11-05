import sys
import os
import pendulum
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

if AIRFLOW_HOME is None:
    AIRFLOW_HOME = "/usr/local/airflow"
# [END import_module]
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

start_date= datetime(2024, 10, 31, 15, 0, tzinfo=local_tz)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'Pi_calc',
    default_args=default_args,
    description='Pi calculation',
    schedule_interval="25 15 * * *",
    start_date=start_date,
    catchup=False,
    tags=['IRD'],
) as dag:

    # step_pi_calc = BashOperator(
    #     task_id='pi_calc',
    #     # bash_command=f"""bash {AIRFLOW_HOME}/dags/repo/dags/submit_pi_calc.sh""",
    #     bash_command="""
    #         echo "Execution Date: ${EXECUTION_DATE}"
    #     """,
    #     dag=dag
    # )


    step_spark_submit_pi = BashOperator(
        task_id='spark_submit_pi',
        bash_command="""spark-submit \
            --class org.apache.spark.examples.SparkPi \
            --master yarn --deploy-mode client \
            --driver-cores 2 \
            --driver-memory 1g \
            --num-executors 3 \
            --executor-cores 2 \
            --executor-memory 1g \
            /opt/spark/examples/jars/spark-examples*.jar 10""",
    )
    # step_pi_calc

    step_spark_submit_pi
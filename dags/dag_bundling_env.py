import sys
import os
import pendulum
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
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
    'bundling_env',
    default_args=default_args,
    description='Bundling environment',
    schedule_interval="25 15 * * *",
    start_date=start_date,
    catchup=False,
    tags=['IRD'],
) as dag:


    step_submit_pyspark = BashOperator(
        task_id='pi_pyspark_client',
        bash_command="""spark-submit \
            --class org.apache.spark.examples.SparkPi \
            --master yarn --deploy-mode client \
            --driver-cores 2 \
            --driver-memory 1g \
            --num-executors 3 \
            --executor-cores 2 \
            --executor-memory 1g \
            --archives /opt/conda/archives/spark.tar.gz#environment \
            {AIRFLOW_HOME}/dags/repo/scripts/pi-spark.py""",
        env={
            "PYSPARK_PYTHON": "./environment/bin/python",
        #     "PYSPARK_DRIVER_PYTHON": "/opt/conda/envs/spark/bin/python",
        }
    )

    # step_submit_pyspark_cluster = BashOperator(
    #     task_id='pi_pyspark_cluster',
    #     bash_command="""spark-submit \
    #         spark-submit \
    #         --class org.apache.spark.examples.SparkPi \
    #         --master yarn --deploy-mode cluster \
    #         --driver-cores 2 \
    #         --driver-memory 1g \
    #         --num-executors 3 \
    #         --executor-cores 2 \
    #         --executor-memory 1g \
    #         --archives /opt/conda/archives/spark.tar.gz#environment \
    #         {AIRFLOW_HOME}/dags/repo/scripts/pi-spark.py""",
    #     env={
    #         "PYSPARK_PYTHON": "./environment/bin/python",
    #     #     "PYSPARK_DRIVER_PYTHON": "/opt/conda/envs/spark/bin/python",
    #     }
    # )

    # step_submit_pyspark_cluster_hdfs = BashOperator(
    #     task_id='pi_pyspark_cluster_hdfs',
    #     bash_command="""spark-submit \
    #         --master yarn --deploy-mode cluster \
    #         --driver-memory 4g --num-executors 6 \
    #         --executor-cores 3 --executor-memory 4g \
    #         --conf spark.driver.maxResultSize=3g \
    #         --archives hdfs://c0s/user/dp-ai-workspace-97ta9/archives/spark.tar.gz#environment \
    #         {AIRFLOW_HOME}/dags/repo/scripts/pi-spark.py""",
    #     env={
    #         "PYSPARK_PYTHON": "./environment/bin/python",
    #     #     "PYSPARK_DRIVER_PYTHON": "/opt/conda/envs/spark/bin/python",
    #     }
    # )
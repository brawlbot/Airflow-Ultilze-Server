import sys
import os
import pendulum
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

if AIRFLOW_HOME is None:
    AIRFLOW_HOME = "/opt/airflow"
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
    'verify_jdbc_version',
    default_args=default_args,
    description='Verify JDBC version',
    schedule_interval="25 15 * * *",
    start_date=start_date,
    catchup=False,
    tags=['IRD'],
) as dag:


    custom = BashOperator(
        task_id='verify_jdbc_version',
        bash_command="""spark-submit \
            --master yarn --deploy-mode cluster \
            --driver-cores 2 \
            --driver-memory 1g \
            --num-executors 3 \
            --executor-cores 2 \
            --executor-memory 1g \
            --archives hdfs://c0s/user/dp-ai-workspace-97ta9/archives/pydoop_env.tar.gz#environment \
            --jars hdfs://c0s/user/dp-ai-workspace-97ta9/jars/postgresql-42.7.4.jar \
            /opt/airflow/dags/repo/scripts/verify_jdbc_version.py
        """,

        env={
            "HADOOP_CLIENT_OPTS": "-Xmx2147483648 -Djava.net.preferIPv4Stack=true",
            "HADOOP_CONF_DIR": "/etc/hadoop",
            "PYSPARK_PYTHON": "./environment/bin/python",
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/hadoop/bin:/opt/hive/bin:/opt/spark/bin",
            "PYTHONPATH": "/opt/spark/python/lib/py4j-0.10.7-src.zip:/opt/spark/python/lib/py4j-0.10.9-src.zip:/opt/spark/python/lib/py4j-0.10.9.3-src.zip:/opt/spark/python:$PYTHONPATH",
            "PYTHONSTARTUP": "/opt/spark/python/pyspark/shell.py",
            "SPARK_HOME": "/opt/spark",
            "SPARK_CONF_DIR": "/etc/spark",
        }
    )
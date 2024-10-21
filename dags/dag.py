from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import pendulum
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
 
if AIRFLOW_HOME is None:
    AIRFLOW_HOME = "/usr/local/airflow"
# [END import_module]
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['hieunt21@vng.com.vn'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
 
with DAG(
    'test_call_os_env',
    default_args=default_args,
    description='test_call_os_env5',
    schedule_interval="40 1-23/2 * * *",
    start_date=datetime(2024, 10, 15, 5, 40, tzinfo=local_tz),
    end_date=datetime(2024, 10, 22, 4, 0, tzinfo=local_tz),  # Stop on 2024-10-22 04:00:00
    catchup=False,
    tags=['test'],
) as dag:
 
    step_tlbb2_weekly_event = BashOperator(
        task_id='test_call_os_env',
        bash_command=f"""bash {AIRFLOW_HOME}/dags/repo/dags/hieunt21/experimental/submit_exp_env_call.sh """,
        dag=dag
    )
 
    step_tlbb2_weekly_event

# has context menu
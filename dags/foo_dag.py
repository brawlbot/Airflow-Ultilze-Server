import sys, os
from pprint import pprint
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.foo import foo  # Importing the foo function

def get_sys_path():
    pprint(sys.path)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG('foo_dag', default_args=default_args, schedule_interval='@daily')

run_foo = PythonOperator(
    task_id='run_foo_task',
    python_callable=foo,  # Calling the foo function
    dag=dag,
)

get_sys_path = PythonOperator(
    task_id='get_sys_path_task',
    python_callable=get_sys_path,
    dag=dag,
)

# get_sys_path >> run_foo
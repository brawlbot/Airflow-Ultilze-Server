import os
import yaml
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.models import DAG

dag = DAG(
    dag_id="dag_main",
    default_args={
        "owner": "airflow",
        "retries": 0,
        "start_date": datetime(2023, 8, 25),
        "retry_delay": timedelta(minutes=5),
        "depends_on_past": False,
    },
    schedule_interval="0 8 * * *",
    catchup=False,
    max_active_runs=1,
)

with dag:
    install_prometheus_task = BashOperator(
        task_id=f"task_install_prometheus",
        bash_command=f"pip install prometheus-api-client",
        dag=dag,
    )

    run_prometheus_task = BashOperator(
        task_id=f"task_pip_list",
        bash_command=f"pip config list -v",
        dag=dag,
    )
    install_prometheus_task
    #install_prometheus_task >> run_prometheus_task

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.models import DAG

dag = DAG(
    dag_id="proxy_http_env_dag",
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
    export_http_proxy = BashOperator(
        task_id="task_export_env_vars",
        bash_command="export HTTP_PROXY=http://proxy.dp.vng.vn:3128 && export HTTPS_PROXY=http://proxy.dp.vng.vn:3128 && curl google.com",
        dag=dag,
    )


    env_vars_check = BashOperator(
        task_id="task_env_vars_check",
        bash_command="env | grep HTTP_PROXY",
        env={
            "HTTP_PROXY": "http://proxy.dp.vng.vn:3128",
            "HTTPS_PROXY": "http://proxy.dp.vng.vn:3128",
        },
        dag=dag,
    )

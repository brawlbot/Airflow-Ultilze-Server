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

    config_pip_list = BashOperator(
        task_id=f"task_pip_list",
        bash_command=f"pip config list -v",
        dag=dag,
    )

    gs2_troubleshoot_conda = BashOperator(
        task_id=f"task_gs2_troubleshoot_conda",
        
        # bash_command=f"conda create -y -n pyspark-airflow python=3.8 sqlalchemy=2.0.21 psycopg2=2.9.3 conda-pack pandas=1.5.2 joblib=1.4.2 pydrive=1.3.1 gspread=6.1.2 && source /opt/conda/bin/activate pyspark-airflow && conda pack -o environment.tar.gz",
        bash_command=f"conda create -y -n pyspark-airflow python=3.8 sqlalchemy=2.0.21 psycopg2=2.9.3 conda-pack pandas=1.5.2 joblib=1.4.2 pydrive=1.3.1 && /opt/conda/envs/pyspark-airflow/bin/pip3 install gspread==6.1.2 && conda pack -o environment.tar.gz",
        dag=dag,
    )

    check_env_task = BashOperator(
        task_id=f"task_check_env",
        # Updated command to ensure conda is initialized before activation
        bash_command=f"source /opt/conda/etc/profile.d/conda.sh && conda activate pyspark-airflow && conda env export > /tmp/environment.yml && cat /tmp/environment.yml",
        dag=dag,
    )
    
    # install_prometheus_task >> gs2_troubleshoot_conda

    config_pip_list >> gs2_troubleshoot_conda >> check_env_task
    # config_pip_list >> install_prometheus_task

    #install_prometheus_task >> run_prometheus_task

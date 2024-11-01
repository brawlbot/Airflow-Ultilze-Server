#!/usr/bin/env bash
set -e

# Global Variables
AIRFLOW_REPO=${AIRFLOW_REPO:="/opt/airflow/dags/repo"}
export AIRFLOW_HOME="/opt/airflow"

# Load Export Configuration
export PATH=/opt/spark/bin:${PATH}
export PYSPARK_PYTHON=./environment/bin/python
# Hadoop Related Options
export HADOOP_HOME=/opt/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=/etc/hadoop
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/nativ"
cd ${AIRFLOW_HOME}

# if [ ! -f environment.tar.gz ]
# then
#     conda create -y -n pyspark-airflow  python=3.8 sqlalchemy=2.0.21 psycopg2=2.9.3 conda-pack pandas=1.5.2 joblib=1.4.2   
#     source /opt/conda/bin/activate pyspark-airflow
#     pip install gspread==6.1.2
#     pip install pydrive==1.3.1
#     conda pack -o environment.tar.gz
# fi
# conda unpack -o environment.tar.gz



# source /home/airflow/.bashrc
# Spark Related Options
export SPARK_HOME="/opt/spark"
# export PYSPARK_PYTHON=./environment/bin/python
EXECUTION_DATE=${1}

echo "Execution Date: ${EXECUTION_DATE}"

spark-submit \
--master yarn \
--deploy-mode cluster \
--name Demo::${EXECUTION_DATE} \
--driver-memory 4g \
--driver-cores 4 \
--executor-memory 4g \
--executor-cores 4 \
--num-executors 3 \
${AIRFLOW_HOME}/dags/repo/dags/pi-spark.py

# --archives environment.tar.gz#environment \
# {
#   "argv": [
#     "/opt/conda/envs/spark/bin/python",
#     "-m",
#     "ipykernel_launcher",
#     "-f",
#     "{connection_file}"
#   ],
#   "display_name": "Spark - Python (YARN Client Mode)",
#   "language": "python",
#   "env": {
#     "HADOOP_CLIENT_OPTS": "-Xmx2147483648 -Djava.net.preferIPv4Stack=true",
#     "HADOOP_CONF_DIR": "/etc/hadoop",
#     "PYSPARK_PYTHON": "./environment/bin/python",
#     "PYSPARK_DRIVER_PYTHON": "/opt/conda/envs/spark/bin/python",
#     "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/hadoop/bin:/opt/hive/bin:/opt/spark/bin",
#     "PYSPARK_SUBMIT_ARGS": "--name PySpark --master yarn --deploy-mode client --driver-memory 4g --num-executors 6 --executor-cores 3 --executor-memory 4g --conf spark.driver.maxResultSize=3g --archives /opt/conda/archives/spark.tar.gz#environment pyspark-shell",
#     "PYTHONPATH": "/opt/spark/python/lib/py4j-0.10.7-src.zip:/opt/spark/python/lib/py4j-0.10.9-src.zip:/opt/spark/python/lib/py4j-0.10.9.3-src.zip:/opt/spark/python:$PYTHONPATH",
#     "PYTHONSTARTUP": "/opt/spark/python/pyspark/shell.py",
#     "SPARK_HOME": "/opt/spark",
#     "SPARK_CONF_DIR": "/etc/spark"
#   },
#   "metadata": {
#     "debugger": true
#   }
# }

cd /tmp
wget https://raw.githubusercontent.com/ovh/data-processing-samples/refs/heads/master/python_calculatePi/pi-spark.py

export HADOOP_CLIENT_OPTS="-Xmx2147483648 -Djava.net.preferIPv4Stack=true"
export HADOOP_CONF_DIR="/etc/hadoop"
export PYSPARK_PYTHON="./environment/bin/python"
export PYSPARK_DRIVER_PYTHON="/opt/conda/envs/spark/bin/python"
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/hadoop/bin:/opt/hive/bin:/opt/spark/bin"
export PYTHONPATH="/opt/spark/python/lib/py4j-0.10.7-src.zip:/opt/spark/python/lib/py4j-0.10.9-src.zip:/opt/spark/python/lib/py4j-0.10.9.3-src.zip:/opt/spark/python:$PYTHONPATH"
export PYTHONSTARTUP="/opt/spark/python/pyspark/shell.py"
export SPARK_HOME="/opt/spark"
export SPARK_CONF_DIR="/etc/spark"

spark-submit \
    --class org.apache.spark.examples.SparkPi \
    --master yarn --deploy-mode client \
    --driver-cores 2 \
    --driver-memory 1g \
    --num-executors 3 \
    --executor-cores 2 \
    --executor-memory 1g \
    --archives /opt/conda/archives/spark.tar.gz#environment \
    pi-spark.py
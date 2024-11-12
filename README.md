# Structure
```log
.
├── README.md
├── airflow-ultilze-server-main.zip
├── config
│   └── cred.json
├── dags
│   ├── dag.py
│   ├── dag_main.py
│   ├── debug_sleep.py
│   ├── foo_dag.py
│   ├── proxy_http_env_dag.py
│   ├── run.sh
│   ├── script.py
│   └── spark_run.py
├── debug.md
├── notebook
│   ├── ageless-period-439309-d8-5b6312c8112e.json
│   ├── create_sc_with_jar.ipynb
│   ├── ggsheet_read.ipynb
│   ├── jdbc_read.py
│   ├── jdbc_write.ipynb
│   ├── pi-spark.py
│   ├── pyspark_jdbc_check.ipynb
│   ├── pyspark_jdbc_check_cluster.ipynb
│   ├── setup_jar.ipynb
│   ├── spark-submit.sh
│   ├── validate-Copy1.ipynb
│   └── validate_data.ipynb
├── scripts
│   └── foo.py
└── spark-submit.sh
```

# 1. Import module from another directory


run [foo_dag](dags/foo_dag.py) to call [foo](scripts/foo.py) function to print `__name__` and `__file__`


# 2. Run airflow with custom environment variable
run [proxy_http_env_dag](dags/proxy_http_env_dag.py) to test if Airflow can run with custom environment variable

# 3. Run spark jdbc
run [pyspark_jdbc_check.ipynb](notebook/pyspark_jdbc_check.ipynb) to test if spark can read from jdbc

# 4. Run spark pi
run [pi_calc_dags](dags/pi_calc_dags.py) to test if spark can run pi calculation with option:

    + client mode (class org.apache.spark.examples.SparkPi with jar and python in the same node)
    + cluster mode (class org.apache.spark.examples.SparkPi with jar and python in different nodes)

# 5. Bundling environment
## 5.1 Create environment
```bash
conda activate /opt/conda/envs/spark
/opt/conda/envs/spark/bin/pip3 install gspread==6.1.4
conda pack -o environment.tar.gz
```
## 5.2 Push environment to hdfs
```bash
klist 
Ticket cache: FILE:/tmp/krb5cc_1000
Default principal: dp-ai-workspace-97ta9-hieunt21@DP.VNG.VN

Valid starting       Expires              Service principal
11/05/2024 15:07:15  11/06/2024 15:07:15  krbtgt/DP.VNG.VN@DP.VNG.VN
        renew until 11/08/2024 03:07:15
```


```sh
jupyter kernelspec list
jupyter kernelspec install /home/jovyan/custom_spark_kernel --user
```

```bash
hadoop dfs -ls hdfs://c0s/user/dp-ai-workspace-97ta9/archives/
hadoop dfs -mkdir -p /user/dp-ai-workspace-97ta9/archives/
hadoop dfs -put environment.tar.gz hdfs://c0s/user/dp-ai-workspace-97ta9/archives/environment.tar.gz
```

## 5.3 Run dag
run [dag_bundling_env](dags/dag_bundling_env.py) to test if spark can import module from bundling environment
    bundling environment (in cluster mode), note that do not set PYSPARK_DRIVER_PYTHON in the environment variable


```sh
yarn logs -applicationId application_1730805578034_23329 > yarn.log
```

```log
/hadoop/yarn/local/usercache/dp-ai-workspace-97ta9/appcache/application_1730805578034_23329/container_e1299_1730805578034_23329_01_000001/environment/lib/python3.8/site-packages/gspread/__init__.py
----------------------------------------------------------------------------------------------------
6.057 seconds elapsed for spark approach and n= 1000
Pi is roughly 3.196000
```


# 6. Run spark with custom jar
run [dag_jars_add](dags/dag_jars_add.py) to test if spark can run with custom jar
```sh
mkdir -p /home/jovyan/jars
wget https://jdbc.postgresql.org/download/postgresql-42.7.4.jar -O /home/jovyan/jars/postgresql-42.7.4.jar
hadoop dfs -put postgresql-42.7.4.jar hdfs://c0s/user/dp-ai-workspace-97ta9/archives/postgresql-42.7.4.jar

hadoop dfs -mkdir -p hdfs://c0s/user/dp-ai-workspace-97ta9/config
hadoop dfs -put config/postgres.json hdfs://c0s/user/dp-ai-workspace-97ta9/config/postgres.json
```

```bash
conda activate /opt/conda/envs/spark/
pip3 install pydoop
conda pack -o pydoop_env.tar.gz
hadoop dfs -put pydoop_env.tar.gz hdfs://c0s/user/dp-ai-workspace-97ta9/archives/pydoop_env.tar.gz
```
## expected output
```log
24/11/12 10:34:45 INFO yarn.Client: Application report for application_1731398263149_2652 (state: RUNNING)
24/11/12 10:34:46 INFO yarn.Client: Application report for application_1731398263149_2652 (state: RUNNING)
24/11/12 10:34:47 INFO yarn.Client: Application report for application_1731398263149_2652 (state: RUNNING)
24/11/12 10:34:48 INFO yarn.Client: Application report for application_1731398263149_2652 (state: RUNNING)
```
```sh
yarn logs -applicationId application_1731398263149_2652 |less
--- df_local ---
----------------------------------------------------------------------------------------------------
   id
0  22
1  23
2  24
3  25
4  26
5  88
6  89
7  90
8  91
9  92
----------------------------------------------------------------------------------------------------
(10, 1)
----------------------------------------------------------------------------------------------------
```
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

    - client mode (jar and python in the same node)
    - cluster mode (jar and python in different nodes)
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

```bash
hadoop dfs -ls hdfs://c0s/user/dp-ai-workspace-97ta9/archives/
hadoop dfs -mkdir -p /user/dp-ai-workspace-97ta9/archives/
hadoop dfs -put environment.tar.gz hdfs://c0s/user/dp-ai-workspace-97ta9/archives/environment.tar.gz
```
## 5.3 Run dag

run [dag_bundling_env](dags/dag_bundling_env.py) to test if spark can run pi calculation with option:

    - bundling environment (jar and python in the same node, but python is in a separated environment)
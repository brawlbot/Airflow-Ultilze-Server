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


# Use case

## 1. Import module from another directory


run [foo_dag](dags/foo_dag.py) to call [foo](scripts/foo.py) function to print `__name__` and `__file__`


## 2. Run airflow with custom environment variable
run [proxy_http_env_dag](dags/proxy_http_env_dag.py) to test if Airflow can run with custom environment variable

## 3. Run spark jdbc
run [pyspark_jdbc_check.ipynb](notebook/pyspark_jdbc_check.ipynb) to test if spark can read from jdbc
from time import time
import os
from random import random
from operator import add
import numpy as np
import pydoop.hdfs as hdfs
import json

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"
spark = SparkSession.builder.appName('VerifyJDBCVersion').getOrCreate()
sc = spark.sparkContext


# print(f"Spark version: {spark.version}")
# print(f"JDBC version: {spark.conf.get('spark.jars')}")

db_cred_fn = "hdfs://c0s/user/dp-ai-workspace-97ta9/config/postgres.json"
with hdfs.open(db_cred_fn) as f:
    cfg = json.load(f)

DBUSER = cfg.get('Connect DB')['DBUSER']
DBPASS = cfg.get('Connect DB')['DBPASS']
DBHOST = cfg.get('Connect DB')['DBHOST']
DBPORT = cfg.get('Connect DB')['DBPORT']
DBNAME = cfg.get('Connect DB')['DBNAME']

df = spark.read.jdbc(url=f"jdbc:postgresql://{DBHOST}:{DBPORT}/{DBNAME}",
                     table="public.tmp_random_data", properties={"user": DBUSER, "password": DBPASS})

df_local = df.limit(10).toPandas()
print("--- df_local ---")
print("-" * 100)
print(df_local)
print("-" * 100)
print(df_local.shape)
print("-" * 100)
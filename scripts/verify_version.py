from time import time
import os
from random import random
from operator import add
import numpy as np

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"
spark = SparkSession.builder.appName('VerifyVersion').config(
    "spark.archives",
    "/home/jovyan/gspread.tar.gz#environment"
    # "hdfs://c0s/user/dp-ai-workspace-97ta9/archives/gspread.tar.gz#environment"
).getOrCreate()
sc = spark.sparkContext


# map reduce example
N_samples = 100000
df = sc.parallelize(range(1, N_samples))
count = df.count()

print(f"Count of {N_samples} numbers: {count}")

import gspread
print(f"gspread version: {gspread.__version__}")
print(f"gspread file: {gspread.__file__}")
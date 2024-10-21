script:
 
import pandas as pd

import numpy as np

import datetime as dt

from datetime import date, datetime, timedelta

from pyspark.sql.functions import *

from pyspark.sql.types import *

from pyspark.sql.window import Window

from pyspark.sql import functions as F

from sqlalchemy import create_engine, Column, Integer, String, Date, Float, DateTime

from sqlalchemy import inspect

from sqlalchemy import MetaData, Table

from sqlalchemy.ext.automap import automap_base

from sqlalchemy.orm import Session

from sqlalchemy import text

import sqlalchemy as sa

from pyspark.conf import SparkConf

from dateutil.relativedelta import relativedelta

import psycopg2

from sqlalchemy.orm import sessionmaker

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.dialects.postgresql import insert

from pyspark.sql import SparkSession, Row

from sqlalchemy.exc import SQLAlchemyError

from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from functools import reduce

import base64

from pyspark.sql.functions import udf
 
# from pydrive.auth import GoogleAuth

# from pydrive.drive import GoogleDrive

from oauth2client.client import GoogleCredentials

import requests

from io import StringIO

import gspread

from oauth2client.service_account import ServiceAccountCredentials

from pprint import pprint as pp

from pandas import DataFrame

import time
 
import os

import json

import re
 
## sc = SparkContext('local')

spark = SparkSession.builder.appName('exp_envos')\
        .getOrCreate()
 
print('start_config')

## Bỏ giới hạn cột

pd.set_option('display.max_columns', None)
print('2')

spark.conf.set("spark.sql.parquet.mergeSchema", "true")
 
## Tối ưu việc chuyển từ Pyspark DataFrame -> Pandas DataFrame (để xuất file)

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
print('3')
 
## Tính năng AQE (có trong spark 3.x) giúp tối ưu chung

spark.conf.set("spark.sql.adaptive.enabled",True)
 
## Tắt ưu tiên sử dụng merge join, qua đó Spark có thể cân nhắc chọn các kiểu join khác sao cho ra hiệu suất tốt nhất

print('3')
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)
 
## Hợp nhất các phân vùng nhỏ lại, tránh để quá nhiều phân vùng nhỏ mà ko có dữ liệu
spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', True)
 
print('3')
## Khi join bảng lớn với bảng nhỏ, nếu mà kích thước bảng nhỏ < 256mb thì tự động chuyển thành broadcast join, phân phối data tới toàn bộ các nút

spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", '100mb')
 
print('3')
## Khi data bị skew, tự động tối ưu hóa khi join

spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)

spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")
 
print('end_config')
 
print('start_run')

max_env_var_length = 2000
 
print('start_call_env')

creds_start = '{'

creds_1 = os.getenv('creds_1')

print('end_call_env')

# Check if the variable is set and print its value

if creds_1:

    print(f"Credentials 1: {creds_1}")

else:
    print("The environment variable 'creds_1' is not set.")
 
print('end_print')
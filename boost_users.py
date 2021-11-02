# Load packages

from pyspark import SparkContext, SparkConf, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode
import pygeohash as pgh
from pyspark.ml.fpm import FPGrowth


# Ingest Cosmose data
path = 's3a://ada-prod-data/etl/service/boost/daily/MY/20210111' #ingesting jan 10 days data 10-19
df_cos = spark.read.parquet(path)

# Ingest brq data

path = 's3a://ada-prod-data/etl/data/brq/raw/eskimi/daily/MY/20210111' #ingesting jan 10 days data 10-19
df_brq = spark.read.format('parquet').load(path)

# enrich Cosmose data with affluence, demographics, lifestage and make sure to drop all else

df_cos1 = df_cos.select('ifa')

# add affluence
path_af = 's3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/MY/202101*'
df_af = spark.read.parquet(path_af).select('ifa','final_score')
df_cos2 = df_cos1.join(df_af, on='ifa', how='left')

# add persona

df_p = spark.read.parquet('s3a://ada-dev/DA_repository/Persona/MY/*/202101/ifa_list/')

df_cos3 = df_cos2.join(df_p, on='ifa', how='left')


# enrich data with lifestange information

ls_path = 's3a://ada-platform-components/lifestages/monthly_predictions/MY/*/2021*/ifa_list/'
df_ls = spark.read.parquet(ls_path).select('ifa','lifestage_name_m1','lifestage_name_m2')

df_cos4 = df_cos3.join(df_ls, on='ifa', how='left')


# left join cosmose data with brq data, add label column for model building purposes

df_cos5 = df_cos4.join(df_brq, on='ifa', how='left')

df_cos6 = df_cos5.withColumn('Boost_User',F.lit('Yes'))

df_cos6 = df_cos6.dropDuplicates()

# Schema of df_cos6 [count = 232,956,533]

root
 |-- ifa: string (nullable = true)
 |-- final_score: float (nullable = true)
 |-- persona_id: string (nullable = true)
 |-- persona_name: string (nullable = true)
 |-- lifestage_name_m1: string (nullable = true)
 |-- lifestage_name_m2: string (nullable = true)
 |-- source: string (nullable = true)
 |-- bundle: string (nullable = true)
 |-- device: string (nullable = true)
 |-- device_category: string (nullable = true)
 |-- platform: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- mccmnc: string (nullable = true)
 |-- carrier: string (nullable = true)
 |-- connection_type: integer (nullable = true)
 |-- ip: string (nullable = true)
 |-- country: string (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- yob: integer (nullable = true)
 |-- gender: string (nullable = true)
 |-- dnt: integer (nullable = true)
 |-- timestamp: long (nullable = true)
 |-- timezone: string (nullable = true)
 |-- Boost_User: string (nullable = false)


''''
prepping brq data with negative class
''''


# filter out ifas that are seen in cosmose

ifa_list = df_cos6.select('ifa').rdd.flatMap(lambda x: x).collect()

df_brq1 = df_brq.filter(~F.col('ifa').isin(ifa_list))

df_brq1 = df_brq1.dropDuplicates()


# sample df_brq

frac = 0.0012

df_brq2 = df_brq1.sample(fraction=frac, seed=31)

# add affluence persona and lifestage

# add affluence
path_af = 's3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/MY/202101*'
df_af = spark.read.parquet(path_af).select('ifa','final_score')
df_brq3 = df_brq2.join(df_af, on='ifa', how='left')

# add persona

df_p = spark.read.parquet('s3a://ada-dev/DA_repository/Persona/MY/*/202101/ifa_list/')

df_brq4 = df_brq3.join(df_p, on='ifa', how='left')


# enrich data with lifestange information

ls_path = 's3a://ada-platform-components/lifestages/monthly_predictions/MY/*/2021*/ifa_list/'
df_ls = spark.read.parquet(ls_path).select('ifa','lifestage_name_m1','lifestage_name_m2')

df_brq5 = df_brq4.join(df_ls, on='ifa', how='left')

# finally add negative class

df_brq6 = df_brq5.withColumn('Boost_User',F.lit('No'))

# columns are in order

df_cos7 = df_cos6.select(sorted(df_cos6.columns))
df_brq7 = df_brq6.select(sorted(df_brq6.columns))


# Final dataset

concat = df_cos7.union(df_brq7)

# write data

concat.coalesce(1).write.format('csv').option('header','true').save('s3a://ada-dev/ishti/boost_proto')

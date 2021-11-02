'''
persona impact on boost user likelihood
'''

# load packages

from pyspark import SparkContext, SparkConf, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode
import pygeohash as pgh

# Ingest Cosmose data
path = 's3a://ada-prod-data/etl/service/boost/daily/MY/2021011*' #ingesting jan 10 days data 10-19
df_cos = spark.read.parquet(path)

# Ingest brq data

path = 's3a://ada-prod-data/etl/data/brq/raw/eskimi/daily/MY/2021011*' #ingesting jan 10 days data 10-19
df_brq = spark.read.format('parquet').load(path)

# only keep ifa from cosmose data then add persona information

df_cos1 = df_cos.select('ifa').distinct()

# add persona

df_p = spark.read.parquet('s3a://ada-dev/DA_repository/Persona/MY/*/202101/ifa_list/')

df_cos2 = df_cos1.join(df_p, on='ifa', how='left').drop('persona_id')

# reshape data

df_cos2 = df_cos2.withColumn('val',lit(1))
df_cos2 = df_cos2.groupBy('ifa').pivot('persona_name').agg(first('val'))
df_cos2 = df_cos2.na.fill(0)

# create positive label


df_cos3 = df_cos2.withColumn('Boost_User',F.lit('Yes'))

df_cos5 = df_cos3.drop('null') # before doing this check printSchema() and see if null column is still there




''''
prepping brq data with negative class
''''


# filter out ifas that are seen in cosmose

ifa_list_boost = df_cos5.select('ifa')
ifa_list_brq = df_brq.select('ifa')

non_boost_ifa = ifa_list_brq.subtract(ifa_list_boost)

df_brq1 = non_boost_ifa.distinct()





# sample df_brq

frac = 0.014   #calculate from scratch everytime

df_brq2 = df_brq1.sample(fraction=frac, seed=31)


# add persona

df_p = spark.read.parquet('s3a://ada-dev/DA_repository/Persona/MY/*/202101/ifa_list/')

df_brq3 = df_brq2.join(df_p, on='ifa', how='left').drop('persona_id')

# reshape data

df_brq3 = df_brq3.withColumn('val',lit(1))
df_brq3 = df_brq3.groupBy('ifa').pivot('persona_name').agg(first('val'))
df_brq3 = df_brq3.na.fill(0).drop('null') # check printschema and see if you need to drop null

# finally add negative class

df_brq4 = df_brq3.withColumn('Boost_User',F.lit('No'))


# columns are in order

df_cos5 = df_cos5.select(sorted(df_cos5.columns))
df_brq4 = df_brq4.select(sorted(df_brq4.columns))

# Final dataset

concat = df_cos5.union(df_brq4)


# write data

concat.coalesce(1).write.format('csv').option('header','true').save('s3a://ada-dev/ishti/boost_persona_impact')

# write again but after dropping ifa

porky = concat.drop('ifa')
porky.coalesce(1).write.format('csv').option('header','true').save('s3a://ada-dev/ishti/boost_persona_impact_dropifas')

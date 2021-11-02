 '''
 Step 1: Get IFA Prediction from Datarobot Model
'''
# load packages

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

# Step 1: Ingest all MY data for 2021

df = spark.read.parquet('s3a://ada-prod-data/etl/data/brq/raw/eskimi/daily/MY/2021*')

distinct_ifas = df.select('ifa').distinct() # get distinct ifas

# Ingest distinct boost ifas
path = 's3a://ada-prod-data/etl/service/boost/daily/MY/2021*' #ingesting jan 10 days data 10-19
df_cos = spark.read.parquet(path)
distinct_cos_ifas = df_cos.select('ifa').distinct()

# Subtract existing boost consumers

virgin_ifas = distinct_ifas.subtract(distinct_cos_ifas)

# inner join with persona data
df_persona = spark.read.parquet('s3a://ada-dev/DA_repository/Persona/MY/*/2021*/ifa_list/').select('ifa','persona_name').distinct()
virgin_ifa_persona = virgin_ifas.join(df_persona, on='ifa', how='inner')

# reshape data for datarobot
df_virgin = virgin_ifa_persona.withColumn('val',lit(1))
df_virgin = df_virgin.groupBy('ifa').pivot('persona_name').agg(first('val'))
df_virgin = df_virgin.na.fill(0)


# get columns in order

df_virgin = df_virgin.select(sorted(df_virgin.columns))

# write file


df_virgin.coalesce(1).write.format('csv').option('header','true').save('s3a://ada-dev/ishti/boost_datarobot_feed')

# drop ifa

df_v = df_virgin.drop('ifa')

# save df_v

df_v.coalesce(1).write.format('csv').option('header','true').save('s3a://ada-dev/ishti/boost_datarobot_feed_dropped_ifa')



# df_virgin ingestion

spark.read.format("csv").option("header","true").load('s3a://ada-dev/ishti/boost_datarobot_feed')
# Check if these IFAs are persistent or disappearing

# Run it by Boost Datarobot to Identify which ones are potential Boost users




# Step 2: Get rid of IFAs that show up in past boost user list

# Subtract any IFA that shows up in cosmose list as far back as possible

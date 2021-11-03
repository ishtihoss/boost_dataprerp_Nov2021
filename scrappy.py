# Import packages

from pyspark import SparkContext, SparkConf, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode
import pygeohash as pgh


# Ingest original data from Boost

df = spark.read.format('csv').option('header','true').load('s3a://ada-dev/ishti/boost_datarobot_feed_broken_into_2/*')

# Ingest part 1 predictions

part_one_pred = spark.read.format('csv').option('header','true').load('s3a://ada-dev/ishti/boost_predictions_4.5m/test_boost_persona_impact_data.csv_Keras_Residual_Cross_Network_Classifier_using_Trai_(24)_100_61824ca83a86fb525fa13cba_83966a85-3135-4729-a30b-2_part-00001-92f8f725-328f-45b2-9add-7d40f15114f7-c0.csv')

part_zero_pred = spark.read.format('csv').option('header','true').load('s3a://ada-dev/ishti/boost_predictions_4.5m_cont/part-00000.csv')


# unite predictions

pred = part_zero_pred.union(part_one_pred)
pred_na_filled = pred.na.fill(0)

# Monotonically increasing id

slim = pred.select('PredictedLabel')
slim = slim.withColumn('stop_id',F.monotonically_increasing_id())

df1 = df.withColumn('stop_id',F.monotonically_increasing_id())

# final file

final = df1.join(slim,on='stop_id',how='left')

# Filter IFAs that are likely to become boost customers

df_x = final.filter(F.col('PredictedLabel')==1)

pbc_ifas = df_x.select('ifa').distinct()

# Save file

pbc_ifas.coalesce(1).write.format('csv').option('header','true').save('s3a://ada-dev/ishti/pbc_ifas')


# Potential boost customers filtered

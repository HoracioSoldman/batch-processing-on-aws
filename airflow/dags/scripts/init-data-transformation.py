#!/usr/bin/env python
# coding: utf-8

# ## One time data transformation
# In this notebook, we are going to transform the stations and weather data in such a way that they will be conformed to the redshift schema for their corresponding tables.
# 
# The preprocessed data will be saved back to S3 before getting loaded to Redshift.

import pyspark
import os

pyspark.__version__

from pyspark.sql import SparkSession

spark = SparkSession.builder\
   .master('local[*]')\
   .appName('data-transformer')\
   .getOrCreate()

sc = spark.sparkContext

df_stations = spark.read.csv("s3a://hrc-de-data/raw/cycling-extras/stations.csv", inferSchema=True, header=True)
df_stations.take(2)

df_stations.printSchema()


from pyspark.sql import functions as F, types as T

# rename columns
stations= df_stations.withColumnRenamed('Station.Id', 'station_id')\
   .withColumnRenamed('StationName', 'station_name')\
   .withColumnRenamed('easting', 'easting')\
   .withColumnRenamed('northing', 'northing') 

stations.show(5)


# count missing values in each column
stations.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in stations.columns]).show()

stations.write.parquet('s3a://hrc-de-data/processed/cycling-dimension/stations/', mode='overwrite')


# ### 2. Weather data

df_weather = spark.read.json("s3a://hrc-de-data/raw/cycling-extras/weather.json")

df_weather.take(2)

df_weather.printSchema()

# drop some columns that we won't need
weather= df_weather.drop('cloudcover', 'conditions', 'datetimeEpoch', 'description', 'dew', 'icon', 
                           'precipcover', 'source', 'stations', 'sunriseEpoch', 'sunsetEpoch')


# transform datetime
weather= weather.withColumnRenamed('datetime', 'weather_date') 
weather= weather.withColumn('weather_date', weather.weather_date.cast(T.DateType()))

weather.printSchema()
print(len(weather.columns), 'columns')


# count missing values in each column
cols= weather.columns
cols.remove('weather_date')

missing_values= weather.select([F.count(F.when(F.col(c).isNull() | F.isnan(c), c)).alias(c) for c in cols])

missing_values.show()


perc_missing_values= weather.select([(F.count(F.when(F.isnan(c) | F.col(c).isNull(), c))/F.count(F.lit(1))).alias(c) for c in cols])
perc_missing_values.show()


# drop columns where missing values are more than 70%

weather= weather.drop('precipprob', 'preciptype', 'snow', 'snowdepth')
weather.columns

weather= weather.repartition(10)

weather.write.parquet('s3a://hrc-de-data/processed/cycling-dimension/weather/', mode='overwrite')

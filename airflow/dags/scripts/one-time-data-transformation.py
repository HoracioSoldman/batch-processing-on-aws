#!/usr/bin/env python
# coding: utf-8

# ## One time data transformation
# In this notebook, we are going to transform the stations and weather data in such a way that they will be conformed to the redshift schema for their corresponding tables.
# 
# The preprocessed data will be saved back to S3 before getting loaded to Redshift.

import pyspark
import os

pyspark.__version__

from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder\
   .appName('data-transformer')\
   .enableHiveSupport()\
   .getOrCreate()


sc = spark.sparkContext
# sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()

df_stations = spark.read.csv("s3a://hrc-de-data/raw/cycling-extras/stations.csv", inferSchema=True, header=True)


# rename columns
stations= df_stations.withColumnRenamed('Station.Id', 'station_id')\
   .withColumnRenamed('StationName', 'station_name')\
   .withColumnRenamed('easting', 'easting')\
   .withColumnRenamed('northing', 'northing') 

# count missing values in each column
stations.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in stations.columns]
   ).show()

stations.write.parquet('s3a://hrc-de-data/processed/cycling-extras/stations/', mode='overwrite')

df_weather = spark.read.json("s3a://hrc-de-data/raw/cycling-extras/weather.json")

# drop some columns that we won't need
weather= df_weather.drop('cloudcover', 'conditions', 'datetimeEpoch', 'description', 'dew', 'icon', 
                            'precipcover', 'source', 'stations', 'sunriseEpoch', 'sunsetEpoch')

# transform datetime
weather= weather.withColumnRenamed('datetime', 'weather_date') 
weather= weather.withColumn('weather_date', weather.weather_date.cast(T.DateType()))

# count missing values in each column
cols= weather.columns
cols.remove('weather_date')
missing_values= weather.select([F.count(F.when(F.col(c).isNull() | F.isnan(c), c)).alias(c) for c in cols])

missing_values.show()

perc_missing_values= weather.select([(F.count(F.when(F.isnan(c) | F.col(c).isNull(), c))/F.count(F.lit(1))).alias(c) for c in cols])
perc_missing_values.show()

# drop columns where missing values are more than 70%

weather= weather.drop('precipprob', 'preciptype', 'snow', 'snowdepth')

weather= weather.repartition(10)

weather.write.parquet('s3a://hrc-de-data/processed/cycling-extras/weather/', mode='overwrite')


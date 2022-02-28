#!/usr/bin/env python
# coding: utf-8

import pyspark
import os

from pyspark.sql import SparkSession, functions as F, types as T


spark = SparkSession.builder.master('local[*]')\
    .appName('journey-and-stations-data-transformer')\
    .enableHiveSupport()\
    .getOrCreate()


df_journey = spark.read.csv("s3a://hrc-de-data/raw/cycling-journey/*/*", inferSchema=True, header=True)

df_journey= df_journey.withColumnRenamed('Rental Id', 'rental_id')\
    .withColumnRenamed('Bike Id', 'bike_id')\
    .withColumnRenamed('Start Date', 'start_date')\
    .withColumnRenamed('End Date', 'end_date')\
    .withColumnRenamed('StartStation Id', 'start_station')\
    .withColumnRenamed('EndStation Id', 'end_station')


# drop unnecessary column
df_journey= df_journey.drop('Duration')

# add the weather_id column
df_journey= df_journey.withColumn('weather_date', F.to_date(df_journey.start_date))

'''
we want to complete the stations data with some additional stations 
which are not present in the original stations data but are seen in some journey.
'''
df_processed_stations= spark.read.parquet('s3a://hrc-de-data/processed/cycling-extras/stations/')

# create temporary table for both weather and journey
df_journey.createOrReplaceTempView('journey')
df_processed_stations.createOrReplaceTempView('stations')


additional_stations= spark.sql('''
select distinct(start_station) as station_id, `StartStation Name` as station_name 
from journey 
where start_station not in (select station_id from stations)
union
select distinct(end_station) as station_id, `EndStation Name` as station_name 
from journey 
where end_station not in (select station_id from stations)
''')
additional_stations.show()

# add columns to the additional stations to avoid errors when merging it to the previous one (df_processed_stations)
additional_stations= additional_stations.withColumn('longitude', F.lit(0)).withColumn('latitude', F.lit(0))\
    .withColumn('easting', F.lit(0)).withColumn('northing', F.lit(0))

additional_stations = additional_stations.withColumn('longitude', additional_stations.longitude.cast(T.DoubleType()))\
    .withColumn('latitude', additional_stations.latitude.cast(T.DoubleType()))\
    .withColumn('easting', additional_stations.easting.cast(T.DoubleType()))\
    .withColumn('northing', additional_stations.northing.cast(T.DoubleType()))

# make sure that the additional stations does not contain duplicated entries
additional_stations.dropDuplicates()


# save stations data into parquet files in s3
additional_stations.write.parquet('s3a://hrc-de-data/processed/cycling-extras/stations/', mode='append')

# drop other unnecessary journey column
df_journey= df_journey.drop('StartStation Name', 'EndStation Name')


# save journey data into parquet files in s3
df_journey.write.parquet('s3a://hrc-de-data/processed/cycling-journey/', mode='append')
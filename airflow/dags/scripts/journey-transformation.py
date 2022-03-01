#!/usr/bin/env python
# coding: utf-8

# ## Transformation for rental journey data 
# This notebook is responsible for transforming journey data by performing the following tasks:
# 
#     1. Renaming columns (removing spaces and lowercasing)
# 
#     2. Convert data types from string to timestamps
#     
#     3. Attach weather dates
#     
#     4. Drop unnecessary columns
#     
#     5. Update extra files for dimension tables

import pyspark
import os

from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master('local[*]') \
    .appName('journey-and-stations-data-transformer')\
    .getOrCreate()

# get journey data
df_journey = spark.read.csv("s3a://hrc-de-data/raw/cycling-journey/*/*", inferSchema=True, header=True)

df_journey.take(2)

df_journey.printSchema()

from pyspark.sql.functions import *
from pyspark.sql.types import *

# rename columns
df_journey= df_journey.withColumnRenamed('Rental Id', 'rental_id').withColumnRenamed('Bike Id', 'bike_id').withColumnRenamed('Start Date', 'start_date').withColumnRenamed('End Date', 'end_date').withColumnRenamed('StartStation Id', 'start_station').withColumnRenamed('EndStation Id', 'end_station')

# convert data types
df_journey= df_journey.withColumn('start_date', to_timestamp(col('start_date'), 'dd/MM/yyy HH:mm'))

df_journey= df_journey.withColumn('end_date',  to_timestamp(col('end_date'), 'dd/MM/yyy HH:mm'))

# add weather_date column
df_journey= df_journey.withColumn('weather_date', to_date(col("start_date"), 'dd/MM/yyy HH:mm'))


df_journey.show(5)
df_journey.printSchema()


# ### Stations data
# We are going to update the stations data (previously saved by another process) with some additional stations that are not present in the original stations data but are seen in some journey.

# read previously saved stations data from parquet
df_processed_stations= spark.read.parquet('s3a://hrc-de-data/processed/cycling-dimension/stations/')

df_processed_stations.tail(2)

# create temporary table for both stations and journey
df_journey.createOrReplaceTempView('journey')
df_processed_stations.createOrReplaceTempView('station')


# we keep all the stations which are not found in the temp view station table
additional_stations= spark.sql('''
select distinct(start_station) as station_id, `StartStation Name` as station_name 
from journey 
where start_station not in (select station_id from station)
union
select distinct(end_station) as station_id, `EndStation Name` as station_name 
from journey 
where end_station not in (select station_id from station)
''')
additional_stations.show()


# add columns to the additional stations to avoid errors when merging it to the previous one (df_processed_stations)
additional_stations= additional_stations.withColumn('longitude', lit(0).cast(DoubleType())).withColumn('latitude', lit(0).cast(DoubleType())).withColumn('easting', lit(0).cast(DoubleType())).withColumn('northing', lit(0).cast(DoubleType()))

additional_stations.show(5)
additional_stations.printSchema()


# remove duplicate values
additional_stations.dropDuplicates()


# save additional stations data into parquet files in s3
additional_stations.write.parquet('s3a://hrc-de-data/processed/cycling-dimension/stations/', mode='append')


# drop other unnecessary journey columns
df_journey= df_journey.drop('StartStation Name', 'EndStation Name', 'Duration')


# ### Datetime
# We are going to create/update datetime data from the start and end date of each journey.

# extract datetime values from the start and the end date
df_datetime_from_start= (
    df_journey.select(
        col('start_date').alias('datetime_id'), 
        year(col('start_date')).alias('year'), 
        month(col('start_date')).alias('month'), 
        dayofmonth(col('start_date')).alias('day'),
        hour(col('start_date')).alias('hour'),
        minute(col('start_date')).alias('minute'),
        second(col('start_date')).alias('second'),
    )
)
df_datetime_from_end= (
    df_journey.select(
        col('end_date').alias('datetime_id'), 
        year(col('end_date')).alias('year'), 
        month(col('end_date')).alias('month'), 
        dayofmonth(col('end_date')).alias('day'),
        hour(col('end_date')).alias('hour'),
        minute(col('end_date')).alias('minute'),
        second(col('end_date')).alias('second'),
    )
)

df_datetime_from_start.show(3)
df_datetime_from_end.show(3)


# combine the dataframes
df_datetime= df_datetime_from_start.union(df_datetime_from_end)

# remove duplicate entries
df_datetime.dropDuplicates()

df_datetime.show(10)


# save datetime data into parquet files in s3
df_datetime.write.parquet('s3a://hrc-de-data/processed/cycling-dimension/datetime/', mode='append')


# finally, save journey data into parquet files in s3
df_journey.write.parquet('s3a://hrc-de-data/processed/cycling-fact/journey/', mode='append')
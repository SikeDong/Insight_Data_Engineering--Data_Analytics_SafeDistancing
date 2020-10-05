#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

spark = SparkSession \
    .builder \
    .appName("how to read csv file") \
    .getOrCreate()

# processing raw data
# read data from S3
df = spark.read.option("escape", "\"").csv("s3a://.../...", header = True)

# select columns needed
df_sd = df.select("origin_census_block_group", "date_range_start", "distance_traveled_from_home", "median_home_dwell_time", "part_time_work_behavior_devices", "full_time_work_behavior_devices", "median_non_home_dwell_time")

# cast data columns type from string to integer
df_sd_num = df_sd.withColumn("distance_traveled_from_home", df_sd["distance_traveled_from_home"].cast('int')).withColumn("median_home_dwell_time", df_sd["median_home_dwell_time"].cast('int')).withColumn("part_time_work_behavior_devices", df_sd["part_time_work_behavior_devices"].cast('int')).withColumn("full_time_work_behavior_devices", df_sd["full_time_work_behavior_devices"].cast('int')).withColumn("median_non_home_dwell_time", df_sd["median_non_home_dwell_time"].cast('int'))

# cast data type to date
df_sd_date = df_sd_num.withColumn("date",to_date(col('date_range_start')).alias('date').cast("date"))
# substract information from fips code and save into state
df_sd_state = df_sd_date.withColumn("State", df.origin_census_block_group.substr(0,2))

# scale up data to daily basis, and in different
df_sd_final = df_sd_state.groupBy("date","State").sum("distance_traveled_from_home", "median_home_dwell_time", "part_time_work_behavior_devices", "full_time_work_behavior_devices", "median_non_home_dwell_time")

# write data into database
df_sd_final.write\
        .format('jdbc')\
        .mode('overwrite')\
        .option('url', 'jdbc:postgresql://.../...')\
        .option('dbtable', 'Your database')\
        .option('user','Your username ')\
        .option('password', 'Your password ')\
        .option('driver','...')\
        .save()

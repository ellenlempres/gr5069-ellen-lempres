# Databricks notebook source
from pyspark.sql.functions import datediff,current_date,avg
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md ### Read in dataset

# COMMAND ----------

df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv', header=True)

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_drivers = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header=True)

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# MAGIC %md ### Transform Data

# COMMAND ----------

df_drivers = df_drivers.withColumn('age', datediff(current_date(),df_drivers.dob))

# COMMAND ----------

df_drivers = df_drivers.withColumn('age', (datediff(current_date(),df_drivers.dob)/365))

# COMMAND ----------

df_drivers = df_drivers.withColumn('age', df_drivers['age'].cast(IntegerType()))

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

df_lap_drivers = df_drivers.select('driverID', 'driverRef', 'code', 'forename', 'surname', 'nationality', 'age').join(df_laptimes, on=['driverId'])

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

df_races = spark.read.csv('s3://columbia-gr5069-main/raw/races.csv', header=True)

# COMMAND ----------

display(df_races)

# COMMAND ----------

df_lap_drivers = df_lap_drivers.join(df_races.select('name', 'year', 'raceId'), on=['raceId'])

# COMMAND ----------

df_lap_drivers = df_lap_drivers.join(df_races.select('name', 'year', 'raceId'), on=['raceId']).drop('raceId', 'driverId')

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md ### Aggregate by Age

# COMMAND ----------

df_agg_age = df_lap_drivers.groupby('age').agg(avg('milliseconds'))

# COMMAND ----------

display(df_agg_age)

# COMMAND ----------

# MAGIC %md ### Storing Data in S3

# COMMAND ----------

df_agg_age.write.csv('s3://eil2113-gr5069/processed/inclass/laptimes_by_age.csv')

# COMMAND ----------



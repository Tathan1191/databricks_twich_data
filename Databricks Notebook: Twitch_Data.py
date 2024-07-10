# Databricks notebook source
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("dbfs:/FileStore/tables/twitchdata_update-2.xls", header=True, inferSchema=True)

# COMMAND ----------

df.head()

# COMMAND ----------

df.head()


# COMMAND ----------

#df_cleaned = df.dropna(subset=["columna1", "columna2"])
                #drop_duplicates()
                #str.replace() 

# COMMAND ----------

#Lista de valores
missing_values = ["--", "n/a", "na", "NaN"]

# COMMAND ----------

#Cleansing:
df = spark.read.option("nullValue", missing_values).csv("dbfs:/FileStore/tables/twitchdata_update-2.xls")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM csv.`/FileStore/tables/twitchdata_update-2.xls`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM csv.`/FileStore/tables/twitchdata_update-2.xls`
# MAGIC LIMIT 5

# COMMAND ----------

# VIZZ

# File location and type
file_location = "/FileStore/tables/twitchdata_update-2.xls"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# Databricks notebook source
"""
# This code block is only for transformation purposes.
# Required each time the cluster is restarted which should be only on the bronze(first) notebook as they run in order.

tiers = ["bronze", "silver", "gold"]
adls_paths = {tier: f"abfss://{tier}@earthquakesa.dfs.core.windows.net/" for tier in tiers}

## Accessing Paths
bronze_adls = adls_paths["bronze"]
silver_adls = adls_paths["silver"]
gold_adls = adls_paths["gold"]


# displaying ls
dbutils.fs.ls(bronze_adls)
dbutils.fs.ls(silver_adls)
dbutils.fs.ls(gold_adls)
"""

# COMMAND ----------

"""
from datetime import date, timedelta

start_date = date.today() - timedelta(1)
end_date = date.today()
"""

# COMMAND ----------

# MAGIC %md
# MAGIC Retrieve the task values from the previos task (bronze notebook)

# COMMAND ----------

# Retrieving the task values from previous task (bronze)

bronze_output = dbutils.jobs.taskValues.get(taskKey = "Bronze", key = "bronze_output", debugValue="")

# Access Individual values
start_date = bronze_output.get("start_date", "")
bronze_adls = bronze_output.get("bronze_adls", "")
silver_adls = bronze_output.get("silver_adls", "")

print(f"Start Date: {start_date} , Bronze ADLS: {bronze_adls}")


# COMMAND ----------

from pyspark.sql.functions import col, isnull, when
from pyspark.sql.types import TimestampType
from datetime import date, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the JSON Data to Spark DataFrame 

# COMMAND ----------

df = spark.read \
  .option("multiline", True) \
  .json(f"{bronze_adls}{start_date}_earthquake_data.json")

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reshape Data
# MAGIC We will now reframe the data as we can see the coordinates shows lat, long and elevation

# COMMAND ----------

# Reshaping the earthquake data

df = (
  df.select(
    "id",
    col("geometry.coordinates").getItem(0).alias("longitude"),
    col("geometry.coordinates").getItem(1).alias("latitude"),
    col("geometry.coordinates").getItem(2).alias("elevation"),
    col("properties.title").alias("title"),
    col("properties.place").alias("place_description"),
    col("properties.sig").alias("sig"),
    col("properties.mag").alias("magnitude"),
    col("properties.magType").alias("magType"),
    col("properties.time").alias("time"),
    col("properties.updated").alias("updated")
  )
)

# COMMAND ----------

df.dtypes

# COMMAND ----------

 df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Data: Check for Missing or Null Values

# COMMAND ----------

df =(
  df
  .withColumn("longitude", when(isnull(col("longitude")),0).otherwise(col("longitude")))
  .withColumn("latitude", when(isnull(col("latitude")),0).otherwise(col("latitude")))
  .withColumn("time", when(isnull(col("time")),0).otherwise(col("time")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert `time` and `update` to timeStamp from unix time

# COMMAND ----------

df = (
  df
  .withColumn("time", (col("time")/1000).cast(TimestampType()))
  .withColumn("updated", (col("updated")/1000).cast(TimestampType()))
)

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save the Transformed dataframe to Silver container.
# MAGIC Since we have now removed the null/missing values. We will now Save the json file to Silver container.

# COMMAND ----------

# Save the  transformed dataframe to Silver container.
silver_output_path = f"{silver_adls}earthquake_events_silver/"

# COMMAND ----------

# Append Dataframe to Silver conatiner in Parquet format
df.write.mode("append").parquet(silver_output_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exiting the notebook and passing the parameter
# MAGIC Pass the parameters through taskValue to Gold Notebook.

# COMMAND ----------

dbutils.jobs.taskValues.set("silver_output",silver_output_path)
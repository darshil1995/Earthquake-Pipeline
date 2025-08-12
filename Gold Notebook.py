# Databricks notebook source
"""
# Required each time the cluster is restarted which should be only on the bronze(first) notebook as they run in order.

tiers = ["bronze", "silver", "gold"]
adls_paths = {tier: f"abfss://{tier}@earthquakesa.dfs.core.windows.net/" for tier in tiers}

## Accessing Paths
bronze_adls = adls_paths["bronze"]
silver_adls = adls_paths["silver"]
gold_adls = adls_paths["gold"]

silver_output = f"{silver_adls}earthquake_events_silver/"

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
# MAGIC ## Fetching data from the previous task (Bronze & Silver Notebook)
# MAGIC

# COMMAND ----------

## Retrieve the task value from previous task (Bronze and Silver Notebook)
bronze_output = dbutils.jobs.taskValues.get(taskKey="Bronze", key="bronze_output", debugValue="")
silver_output = dbutils.jobs.taskValues.get(taskKey="Silver", key="silver_output", debugValue="")


## Accessing individual variables
start_date = bronze_output.get("start_date","")
silver_adls = bronze_output.get("silver_adls","")
gold_adls = bronze_output.get("gold_adls","")

print(f"Start Date: {start_date}, Gold ADLS: {gold_adls}")

# COMMAND ----------

from pyspark.sql.functions import col, udf,when
from pyspark.sql.types import StringType
from datetime import date, timedelta
# Ensuring below library is installed on the cluster
import reverse_geocoder as rg

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the Parquet file
# MAGIC Read the parquet file from Silver Container.
# MAGIC
# MAGIC Note:
# MAGIC - 1)In Bronze container, we will have multiple files which will be saved daily.
# MAGIC - 2)In Silver container, we will have only one parquet file which will be appended everytime a new data is available from bronze
# MAGIC - 3)In Gold layer, we will be filtering only for the data which is greater that start_date, so that we get the earthquake data for 1 day to the start_date.
# MAGIC

# COMMAND ----------

df = spark.read.parquet(silver_output).filter(col('time') > start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC Limiting the data to 100
# MAGIC - This is added to speed up the processings as during testing it was proving a bottleneck
# MAGIC - The problem is caused by Python UDF (reverse_geoencoder) being a bottleneck due to its non-parallel nature and high computational per task.

# COMMAND ----------


df = df.limit(100)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Function to get the country code for given Lat and Long
# MAGIC Spark cannot detect the function of Python directly. Instead it needs to be registered as UDF (User defined function) to access it.Hence created a function and then registered UDF.

# COMMAND ----------

def get_country_code(lat, long):
  """
  Returns the country code for a given latitude and longitude

  parameters:
  lat (float or str): latitude of the location
  lon (float or str): longitude of the location

  Returns:
  str: Country code for the location. retrieved using reverse_geocode API

  Example:
  >>> get_country_code(40.7128, -74.0060)
  'US' 
  """

  try:
    coordinates = (float(lat), float(long))
    result = rg.search(coordinates)[0].get('cc')
    print(f"Processed Coordinates: {coordinates} -> {result}")
    return result

  except Exception as e:
    print(f"Error processing coordinates: {lat}, {lon} -> {str(e)}")
    return None

# COMMAND ----------

## Registering the UDFs(User define function) so they can be used in Dataframes

get_country_code_udf = udf(get_country_code, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC Adding the country Code column using UDF 

# COMMAND ----------

df_with_location = df.\
    withColumn('country_code', get_country_code_udf(col('latitude'), col('longitude')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding Significance(Sig) Classification

# COMMAND ----------

df_with_location_sig_class = df.\
  withColumn("sig", 
             when(col("sig")<100, 'Low').\
               when((col("sig")>=100) & (col("sig")<500), 'Moderate').\
                 otherwise("High")
                 )

# COMMAND ----------

df_with_location_sig_class.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving the data to Gold container.

# COMMAND ----------

# Save the transformed data to Gold container
gold_output_path = f"{gold_adls}earthquake_events_gold/"

# COMMAND ----------

# Append daatafrae to gold container in Paquet format
df_with_location_sig_class.write.mode("append").parquet(gold_output_path)
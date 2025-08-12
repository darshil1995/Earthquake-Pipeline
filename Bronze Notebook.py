# Databricks notebook source
# MAGIC %md
# MAGIC ## Fetching the external ADLS data paths

# COMMAND ----------

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

# COMMAND ----------

import requests
import json
from datetime import date, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting Start and End Date
# MAGIC Getting start and end date is required: https://earthquake.usgs.gov/fdsnws/event/1/#:~:text=2014%2D01%2D02-,query,-to%20submit%20a 
# MAGIC - The query parameter requires Start and end date to fetch the data.

# COMMAND ----------

start_date = date.today() - timedelta(1)
end_date = date.today()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetching Data 
# MAGIC Once fetched, save the json file into bronze container.

# COMMAND ----------

# Construct the API with start and end date provided by Data Factory formatted for geoJSON Output
url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"

try:
  # Make the GET request to fetch data
  response = requests.get(url)

  # Check if the status was successful
  response.raise_for_status()     # Raises HTTPError for bad requests (4xx or 5xx)
  data = response.json().get("features", [])

  # Check if data was collected:
  if not data:
    print("No data was returned for the specified date range")
  else:
    # Specify the ADLS path
    file_path = f"{bronze_adls}/{start_date}_earthquake_data.json"

    # Save the JSON data
    json_data = json.dumps(data, indent=4) 
    dbutils.fs.put(file_path, json_data, overwrite=True)
    print(f"Data successfully saved to {file_path}")

except requests.RequestException as e:
  print(f"Error fetching data: {e}")

# COMMAND ----------

data[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exiting the notenook and passing the parameter.
# MAGIC
# MAGIC Now when we want to go to silver container, we would want to know the path of bronze container, so that we can get the data.
# MAGIC Now you could go to silver notebook and repeat the same process again. but the problem is if you change one of them, you will have to change the others as well.
# MAGIC
# MAGIC So its the standard principle or Data engineering to never repeat ourself.
# MAGIC
# MAGIC -** The best process is to pass the data in our current notebook as a python dictionary data as a parameter during the exit process**
# MAGIC - `**_ We will create the python dictionary of the objects (start_date,end_date, bronze_adls, silver_adls, gold_adls) and pass the parameter using Taskvalues`_**

# COMMAND ----------

# Define the output data dictionary
output_data = {
   "start_date" : start_date.isoformat(),
   "end_date" : end_date.isoformat(),
   "bronze_adls" : bronze_adls,
   "silver_adls" : silver_adls,
   "gold_adls" : gold_adls
}

# Pass the output data to the next notebook
dbutils.jobs.taskValues.set(key="bronze_output",value= output_data)


# COMMAND ----------

# MAGIC %md
# MAGIC Each notebook as we run it in the workflows is considered as a task.
# MAGIC So
# MAGIC - one workflow is a job
# MAGIC - one job is made of many tasks
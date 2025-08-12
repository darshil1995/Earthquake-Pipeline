# Earthquake Pipeline

## 📌 Overview
This project implements an **automated data engineering pipeline** for earthquake data using Azure services.  
It ingests seismic event data from the [USGS Earthquake API](https://earthquake.usgs.gov/fdsnws/event/1/), processes it into clean and enriched datasets, and makes it ready for analysis or visualization. This project demonstrates how to build a scalable data engineering pipeline on Azure using Databricks, Data Lake, Data Factory, and other services.

Government agencies, research institutions, and insurance companies can use this pipeline to:
- Monitor seismic events in near real-time.
- Plan emergency responses.
- Assess risk for infrastructure and policies.

---

## 🏗 Architecture

### High-Level Flow:
1. **Data Ingestion** – Azure Data Factory fetches earthquake data daily from USGS API.  
2. **Data Processing** – Azure Databricks transforms raw data into Bronze, Silver, and Gold layers using medallion architecture.  
3. **Data Storage** – Azure Data Lake stores datasets in structured Parquet format.  
4. **Data Analysis** – Azure Synapse Analytics enables fast querying.  (Under Progress)
5. **Optional Visualization** – Power BI or other BI tools can connect for dashboards. (Under Progress)  


## 📂 Data Layers (Medallion Architecture)

1. **Bronze Layer**  
   - Raw API data stored in Parquet format.  
   - Multiple daily files stored for historical replay.  

2. **Silver Layer**  
   - Cleaned & normalized data (duplicates removed, missing values handled).  
   - Single Parquet file that is appended with new data.  

3. **Gold Layer**  
   - Aggregated and enriched data (e.g., country codes).  
   - Filtered for only data greater than the `start_date` (i.e., last day of data).

---

## 🔗 API Details
- **Base URL:** [https://earthquake.usgs.gov/fdsnws/event/1/](https://earthquake.usgs.gov/fdsnws/event/1/)  
- **Dynamic Parameters:**  
  - `start_date` and `end_date` set via ADF for daily ingestion.

---

## 🚀 Implementation Steps

### 1️⃣ Azure Setup
1. Create Azure account.
2. Set up Azure Databricks (Standard LTS tier).
3. Create Azure Data Lake Storage with **hierarchical namespace enabled**.
4. Create containers: `bronze`, `silver`, `gold`.
5. Assign **Storage Blob Data Contributor** role to the Databricks access connector.

### 2️⃣ Databricks Configuration
1. Launch workspace and start compute.
2. Create storage credentials & external locations for each container.
3. Create separate notebooks for Bronze, Silver, Gold processing.
4. Install required libraries (`reverse_geocoder` for Gold layer).

### 3️⃣ Azure Data Factory
1. Create an ADF pipeline chaining Bronze → Silver → Gold notebooks.
2. Pass dynamic date parameters.
3. Schedule daily runs.

________________________________________
🛠 Planned Work
1. Connect Azure Synapse Analytics for querying transformed datasets
2. Create Power BI dashboards for Earthquake insights
________________________________________
✅ Key Benefits
•	Automation – No manual data pulling, fully orchestrated in ADF.
•	Scalability – Handles large volumes of API data.
•	Actionable Insights – Ready-to-use structured data for stakeholders.
________________________________________
📌 Notes
•	Bronze: Multiple daily files saved.
•	Silver: Single Parquet file appended daily.
•	Gold: Only includes data for 1 day from start_date.
________________________________________
Reference:
https://www.linkedin.com/in/lukejbyrne/ 

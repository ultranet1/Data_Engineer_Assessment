# A. INTRODUCTION

There different methods to implement solutions to this assessment. However I will implement with 2 methods:

## 1. Notebooks (Google Colab, Jupyter Notebooks, Databricks worlflow ETC)

## 2. Workflow Orchestrator and Scheduler like Apache Airflow, Luigi, dagster ETC.

## 1. Notebooks (Google Colab)
Google colab is faster and easier for  writing ETL data pipeline due to its pre-installed libraries and inbuilt settings. 
Although Scheduling & monitoring is also possible in Notebooks (Via Apscheduler, logging and the likes). Its not recommended bcos it lacks proper workflow orchestration and monitoring. (Except databricks workflow, or GCP dataflow).
To access the notebook, please check the file ALARAN_SENIOR_DATA_ENGINEER_ASSESMENT.ipynb or link https://colab.research.google.com/drive/1NVT4aKKjN3tox78GL8CW1PRp68h0jEYM

## 2. Apache Airflow
Its an open-source wokflow scheduler and orchestrator for batch-based etl pipelines. It provides proper monitoring and  scheduling with both UI and CLI.
It can be setup in various ways:
* Linux or WSL
* Virtual Environment
* Docker
For fast setup I will go for: Docker
To setup Apache Airflow Please check: https://github.com/cordon-thiago/airflow-spark/tree/master

# B STEPS
For steps and codes, check google colab notebook ALARAN_SENIOR_DATA_ENGINEER_ASSESMENT.ipynb OR https://colab.research.google.com/drive/1NVT4aKKjN3tox78GL8CW1PRp68h0jEYM
Note: Selenium is used for automatically scraping data from web ( https://doc.clickup.com/3627772/p/h/3epqw-172805/3c092c33c63dada )

TASK A: (SQL QUERY FOR REVENUE GROWTH RATE)
## i. Install and import the necessary libraries (pyspark. selenium, polars & apscheduler)
## ii. Setup and configure Pyspark, airflow, and Selenium
## iii. Use Selenium to automatically scrape data from clickup.com (Schema query, tables, columns and data)  https://doc.clickup.com/3627772/p/h/3epqw-172805/3c092c33c63dada
## iv. Extract and parse text from Schema query including table name, columns and data Type (with regex)
## v. Create Spark dataframe and populate with artificial data
## vi. Convert dataframe to Spark SQL table
## vii. Write SQL query to calculate the revenue MOM Growth rate (2 options:
       * Segmented queries
       * Single query with CTE
## viii. save result as RevenueGrowthRate(MOM).csv

TASK B: PYSPARK OPERATIONS FOR DAILY UTILIZATION OF VEHICLE
## i & ii install, import and setup libraries
## iii. Use Selenium to automatically scrape tables, columns and data from  https://doc.clickup.com/3627772/p/h/3epqw-172805/3c092c33c63dada
## iv. USe polars to automatically infer schema and convert to spark dataframe (Polars is a combination of Pyspark + Pandas)
## v. Clean and Join tables
       * Fill NULL with 0
       * Trim and strip column names
       * specify data types for each columns
       * Rename columns
       * Right join vehicle dataframe with history on vehicle_id column = vehicle_history
       * left join vehicle_history with location dataframe on location_id = vehicle_location
## vi. Define status list and filter based on status
## vii. Calculate duration column from 'start' and 'end' column
## viii. Group Vehicle status by Date, and vehicle id, then aggregate the sum for each date and vehicle 
(convert result to 4 decimal places)
## ix. Perform utilization calculation on the result based on status (convert result to 1 decimal place)
## x. Save result as DailyUtilizationOfEachVehicle.csv
Note: CSV is used bcos of easy readability, incase of big data, Apache Parquet is recommended.

TASK C: SCHEDULING
Its done in both Google colab and Apache Airflow
* Google Colab: Using Apscheduler to run script function daily
* For Apache Airflow: Please check airflow link above on how to create and schedule dag in airflow


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
For fast setup I will go for: Virtual Environment, with sqlite and LocalExecutor 
Note: This is not recommended for Production environment. For production use Docker and Postgresql with CeleryExecutor

To setup Apache Airflow Please check:

# B STEPS
For steps and codes, check google colab notebook ALARAN_SENIOR_DATA_ENGINEER_ASSESMENT.ipynb
Note: Selenium is used for automatically scraping data from web ( https://doc.clickup.com/3627772/p/h/3epqw-172805/3c092c33c63dada )

TASK A: (SQL QUERY FOR REVENUE GROWTH RATE)
## i. Install and import the necessary libraries (pyspark. selenium, polars & apscheduler)
## ii. Setup and configure Pyspark, airflow, and Selenium
## iii. Use Selenium to automatically scrape data from clickup.com (Schema query, tables, columns and data)
## iv. Extract and parse text from Schema query including table name, columns and data Type (with regex)
## v. Create Spark dataframe and populate with artificial data
## vi. Convert dataframe to Spark SQL table
## vii. Write SQL query to calculate the revenue MOM Growth rate (2 options:
       * Segmented queries
       * Single query with CTE
## viii. save result as RevenueGrowthRate(MOM).csv

TASK B: PYSPARK OPERATIONS FOR DAILY UTILIZATION OF VEHICLE
##i & ii install, 


#A. INTRODUCTION

There different methods to implement solutions to this assessment. However I will implement with 2 methods:

##1. Notebooks (Google Colab, Jupyter Notebooks, Databricks worlflow ETC)

##2. Workflow Orchestrator and Scheduler like Apache Airflow, Luigi, dagster ETC.

##1. Notebooks (Google Colab)
Google colab is faster and easier for  writing ETL data pipeline due to its pre-installed libraries and inbuilt settings. 
Although Scheduling & monitoring is also possible in Notebooks (Via Apscheduler, logging and the likes). Its not recommended bcos it lacks proper workflow orchestration and monitoring. (Except databricks workflow, or GCP dataflow).
To access the notebook, please check the file ALARAN_SENIOR_DATA_ENGINEER_ASSESMENT.ipynb or link https://colab.research.google.com/drive/1NVT4aKKjN3tox78GL8CW1PRp68h0jEYM

##2. Apache Airflow
Its an open-source wokflow scheduler and orchestrator for batch-based etl pipelines. It provides proper monitoring and  scheduling with both UI and CLI.
It can be setup in various ways:
* Linux or WSL
* Docker
For fast setup we will go for:

To setup Apache Airflow Please check:

##3 STEPS
###A. INSTALL AND IMPORT THE NECESSARY LIBRAIES (pyspark. slenium





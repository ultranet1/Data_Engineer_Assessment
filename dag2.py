''' #1. IMPORT NECESSARY LIBRARIES
'''
# Import airflow functions
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
# import pyspark functions
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
# import selenium sub-modules and functions for web scraping
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
# Import regex for text parsing, sys for path specification and time for delaying execution
import re
import os
import sys
import time
import random
from datetime import date, timedelta, datetime
# Import polars for automatic schema infering (suitable for big data, unlike pandas.)
# has similar syntax to pyspark
import polars as pl
# import findspark to find spark config
import findspark

''' #2. SETUP AND CONFIGURE PYSPARK, SELENIUM AND AIRFLOW '''
# Selenium configuration
sys.path.insert(0,'/usr/lib/chromium-browser/chromedriver')
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.binary_location = '/usr/bin/chromium-browser'
chrome_options.add_argument("start-maximized")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)
wd = webdriver.Chrome(options=chrome_options )
# Spark configuration
import sys
sys.path.insert(0, "/usr/local/airflow/.local/lib/python3.7/site-packages")
findspark.init()
findspark.find()
spark = SparkSession.builder.master('local[*]') \
                    .appName('test') \
                    .enableHiveSupport() \
                    .getOrCreate()
# Airflow Configuration
start = datetime.datetime(2024,10,8)
default_args = {
    'owner' : 'Alaran_Ibrahim',
    "depends_on_past" : False,
    'retries': 1,
    "start_date"      : start
     }
dag = DAG(dag_id='VehicleUtilization_per_day', catchup=False, schedule_interval= "@daily", default_args=default_args)

'''#3. DEFINE FUNCTION TO EXTRACT, CLEAN & TRANSFORM DATA'''
# Define function to automatically scrape schema query and populate with artificial data
def extract_data():
    # Chrome driver to visit clickup site to scrape queries and table
    wd = webdriver.Chrome(options=chrome_options )
    wd.get('https://doc.clickup.com/3627772/p/h/3epqw-172805/3c092c33c63dada')
    # Wait for all page elements to load
    time.sleep(5)
    # Find specific tables in the web page
    rows = wd.find_elements(By.XPATH, "//table[@class='clickup-table']/tbody/tr")
    table_data = []
    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        row_data = [col.text for col in cols]
        table_data.append(row_data)

    # Extract Vehicles columns and data
    table_1 = table_data[0][0]
    table_1_cols = table_data[1]
    table_1_data = table_data[2]
    table_1_df = pl.DataFrame({str(table_1_cols[0]): [table_1_data[0]],
                            str(table_1_cols[1]): [table_1_data[1]],
                            str(table_1_cols[2]): [table_1_data[2]],
                            str(table_1_cols[3]): [table_1_data[3]],
                            str(table_1_cols[4]): [table_1_data[4]],
                            str(table_1_cols[5]): [table_1_data[5]],
                            str(table_1_cols[6]): [table_1_data[6]],
                            str(table_1_cols[7]): [table_1_data[7]]
                            })
    table_1_df = spark.createDataFrame(table_1_df.to_pandas(), table_1_cols)
    vehicles_df = table_1_df
    vehicles_df.show()

    # Extract Vehicle History columns and data
    table_2 = table_data[3][0]
    table_2_cols = table_data[4]
    table_2_data = table_data[5:14]
    table_2_df = pl.DataFrame({
        str(table_2_cols[0]): [row[0] for row in table_2_data],
        str(table_2_cols[1]): [row[1] for row in table_2_data],
        str(table_2_cols[2]): [row[2] for row in table_2_data],
        str(table_2_cols[3]): [row[3] for row in table_2_data],
        str(table_2_cols[4]): [row[4] for row in table_2_data],
        str(table_2_cols[5]): [row[5] for row in table_2_data],
        str(table_2_cols[6]): [row[6] for row in table_2_data],
        str(table_2_cols[7]): [row[7] for row in table_2_data],
        str(table_2_cols[8]): [row[8] for row in table_2_data]
    })
    table_2_df = spark.createDataFrame(table_2_df.to_pandas())
    vehicles_history_df = table_2_df
    vehicles_history_df.show()

    # Extract location table data and columns
    table_3 = table_data[14][0]
    table_3_cols = table_data[15]
    table_3_data = table_data[16]
    table_3_df = pl.DataFrame({
        str(table_3_cols[0]): [table_3_data[0]],
        str(table_3_cols[1]): [table_3_data[1]],
        str(table_3_cols[2]): [table_3_data[2]],
        str(table_3_cols[3]): [table_3_data[3]],
        str(table_3_cols[4]): [table_3_data[4]],
        str(table_3_cols[5]): [table_3_data[5]],
        str(table_3_cols[6]): [table_3_data[6]]
        })
    table_3_df = table_3_df.to_pandas()
    table_3_df = spark.createDataFrame(table_3_df)
    location_df = table_3_df
    location_df.show()

    # Save/Checkpoint the 3 tables
    vehicles_df.write.csv("Vehicles.csv", mode="Overwrite")
    vehicles_history_df.write.csv("Vehicles_history.csv", mode="Overwrite")
    location_df.write.csv("Location.csv", mode="Overwrite")
    
# Function to clean and join the tables
def clean_and_join():
    vehicles_df = spark.read.csv("Vehicles.csv")
    vehicles_history_df = spark.read.csv("Vehicles_history.csv")
    location_df = spark.read.csv("Location.csv")

    # Remove any leading/trailing spaces from column names in both DataFrames
    vehicles_df = vehicles_df.toDF(*[c.strip() for c in vehicles_df.columns])
    vehicles_history_df = vehicles_history_df.toDF(*[c.strip() for c in vehicles_history_df.columns])
    location_df = location_df.toDF(*[c.strip() for c in location_df.columns])

    ## Rename Vehicle columns
    vehicles_df = vehicles_df.withColumnRenamed("status", "status(vehicle)") \
                            .withColumnRenamed("created_at", "created_at(vehicle)") \
                            .withColumnRenamed("updated_at", "updated_at(vehicle)") \
                            .withColumnRenamed("deleted_at", "deleted_at(vehicle)") \
                            .withColumnRenamed("location_id", "location_id(vehicle)")
    # Rename Vehicle History columns
    vehicles_history_df = vehicles_history_df.withColumnRenamed("created_at", "created_at(vehicle_history)") \
                                            .withColumnRenamed("updated_at", "updated_at(vehicle_history)")\
                                            .withColumnRenamed("deleted_at", "deleted_at(vehicle_history)")\
                                            .withColumnRenamed("status", "status(vehicle_history)")\
                                            .withColumnRenamed("start_time", "start_time(vehicle_history)")\
                                            .withColumnRenamed("end_time", "end_time(vehicle_history)")\
                                            .withColumnRenamed("id", "history_id")
    # Rename Location Columns
    location_df = location_df.withColumnRenamed("created_at", "created_at(location)")\
                            .withColumnRenamed("updated_at", "updated_at(location)")\
                            .withColumnRenamed("deleted_at", "deleted_at(location)")\

    # Right join vehicles and vehicle_history tables
    vehicles_joined = vehicles_df.join(vehicles_history_df, on='vehicle_id', how="right")
    # Left join vehicles_joined and location tables
    vehicles_location = vehicles_joined.join(location_df, on='location_id', how="left")

    # Change columns data type
    column_type_mapping = {
        "vehicle_id": IntegerType(),
        "location_id": IntegerType(),
        "latitude": DoubleType(),
        "longitude": DoubleType(),
        "vehicle_name": StringType(),
        "license_plate_number": StringType(),
        "status(vehicle)": StringType(),
        "created_at(vehicle)": TimestampType(),
        "updated_at(vehicle)": TimestampType(),
        "deleted_at(vehicle)": TimestampType(),
        "history_id": IntegerType(),
        "vehicle_id": IntegerType(),
        "location_id(vehicle)": IntegerType(),
        "start_time(vehicle_history)": TimestampType(),
        "end_time(vehicle_history)": TimestampType(),
        "status(vehicle_history)": StringType(),
        "created_at(vehicle_history)": TimestampType(),
        "updated_at(vehicle_history)": TimestampType(),
        "deleted_at(vehicle_history)": TimestampType(),
        "location_name": StringType(),
        "created_at(location)": TimestampType(),
        "updated_at(location)": TimestampType(),
        "deleted_at(location)": TimestampType()
    }

    # Iterate through the column type mapping and cast each column
    for column_name, data_type in column_type_mapping.items():
        vehicles_location = vehicles_location.withColumn(
            column_name, vehicles_location[column_name].cast(data_type)
        )
    vehicles_location.show()

    # Save and CheckPoint df
    vehicles_location.write.csv("Vehicles_location.csv", mode='Overwrite')
    print('task 2 completed')

# Function to transform and calculate daily utilization of vehicle
def calculate_and_transform():
    # Define status and filter
    all_status = ['YARD', 'ONRENT', 'REPLACEMENT', 'DRIVE_CAR', 'CLEANING', 'RELOCATION', 'TRANSIT', 'PANELSHOP', 'MAINTENANCE', 'PENDING', 'OVERDUE']
    yard_status = ['YARD']
    all_status_df = vehicles_location.filter(vehicles_location['status(vehicle)'].isin(all_status))

    # Calculate duration column from start time and end time
    duration_df = all_status_df.withColumn(
        "duration",
        (unix_timestamp(col("end_time(vehicle_history)")) - unix_timestamp(col("start_time(vehicle_history)"))) /3600
    )
    # Group by date and sum of(duration, status) and calculate the total duration for each vehicle
    duration_by_vehicle = duration_df.groupBy([(to_date("created_at(vehicle_history)")).alias('created_date'),"vehicle_id", "vehicle_name", "license_plate_number", "location_name"]) \
                                    .agg(sum("duration").alias("available_utilization_hours"),
                                        sum(when(col("status(vehicle_history)") == "YARD", col("duration"))).alias("idle_in_yard(hrs)"),
                                        sum(when(col("status(vehicle_history)") == "ONRENT", col("duration"))).alias("on_rent(hrs)"),
                                        sum(when(col("status(vehicle_history)") == "REPLACEMENT", col("duration"))).alias("replacement(hrs)"),
                                        sum(when(col("status(vehicle_history)") == "DRIVE_CAR", col("duration"))).alias("drive_car(hrs)"),
                                        sum(when(col("status(vehicle_history)") == "CLEANING", col("duration"))).alias("cleaning(hrs)"),
                                        sum(when(col("status(vehicle_history)") == "RELOCATION", col("duration"))).alias("relocation(hrs)"),
                                        sum(when(col("status(vehicle_history)") == "TRANSIT", col("duration"))).alias("transit(hrs)"),
                                        sum(when(col("status(vehicle_history)") == "PANELSHOP", col("duration"))).alias("panelshop(hrs)"),
                                        sum(when(col("status(vehicle_history)") == "MAINTENANCE", col("duration"))).alias("maintenance(hrs)"),
                                        sum(when(col("status(vehicle_history)") == "PENDING", col("duration"))).alias("pending(hrs)"),
                                        sum(when(col("status(vehicle_history)") == "OVERDUE", col("duration"))).alias("overdue(hrs)"),
                                    )

    # fill NULL data with 0
    duration_by_vehicle = duration_by_vehicle.fillna(0)

    # Round float data to 4 decimal places
    duration_by_vehicle = duration_by_vehicle.withColumn("available_utilization_hours", round(col("available_utilization_hours"), 4)) \
                                            .withColumn("idle_in_yard(hrs)", round(col("idle_in_yard(hrs)"), 4)) \
                                            .withColumn("on_rent(hrs)", round(col("on_rent(hrs)"), 4)) \
                                            .withColumn("replacement(hrs)", round(col("replacement(hrs)"), 4)) \
                                            .withColumn("drive_car(hrs)", round(col("drive_car(hrs)"), 4)) \
                                            .withColumn("cleaning(hrs)", round(col("cleaning(hrs)"), 4)) \
                                            .withColumn("relocation(hrs)", round(col("relocation(hrs)"), 4)) \
                                            .withColumn("transit(hrs)", round(col("transit(hrs)"), 4)) \
                                            .withColumn("panelshop(hrs)", round(col("panelshop(hrs)"), 4)) \
                                            .withColumn("maintenance(hrs)", round(col("maintenance(hrs)"), 4)) \
                                            .withColumn("pending(hrs)", round(col("pending(hrs)"), 4)) \
                                            .withColumn("overdue(hrs)", round(col("overdue(hrs)"), 4))

    # Calaculate the utilations based on duration status
    duration_by_vehicle = duration_by_vehicle.withColumn("utilization(%)", (col("on_rent(hrs)") + col("replacement(hrs)") + col("overdue(hrs)")) / col('available_utilization_hours') * 100) \
                                            .withColumn("drive_car_utilization(%)", (col("drive_car(hrs)")) / col('available_utilization_hours') * 100) \
                                            .withColumn("ops_utilization(%)", (col("cleaning(hrs)") + col("relocation(hrs)") + col("transit(hrs)") + col("panelshop(hrs)") + col("maintenance(hrs)") + col("pending(hrs)")) / col('available_utilization_hours') * 100) \
                                            .withColumn("idle_utilization(%)", col("idle_in_yard(hrs)") / col('available_utilization_hours') * 100)

    # COnvert utilizations columns to 1 decimal place
    duration_by_vehicle = duration_by_vehicle.withColumn("utilization(%)", format_number(col("utilization(%)"), 0)) \
                                            .withColumn("drive_car_utilization(%)", format_number(col("drive_car_utilization(%)"), 0)) \
                                            .withColumn("ops_utilization(%)", format_number(col("ops_utilization(%)"), 0)) \
                                            .withColumn("idle_utilization(%)", format_number(col("idle_utilization(%)"), 0))


    duration_by_vehicle.show()    

'''#4 DEFINE TASKS AND DEPENDENCIES'''
t1 = PythonOperator(task_id = 'extract_data',
    python_callable=extract_data,
    dag=dag)
t2 = PythonOperator(task_id = 'Clean_and join_data',
    python_callable=clean_and_join,
    dag=dag)
t3 = PythonOperator(task_id='calculate_and_trasform',
    python_callable=calculate_and_transform,
    dag=dag)

# Define dependencies
t1 >> t2 >> t3
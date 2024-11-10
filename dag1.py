''' #1. IMPORT NECESSARY LIBRARIES
'''
# Import airflow functions
#from airflow import DAG
#from airflow.operators.bash_operator import BashOperator
#from airflow.operators.python_operator import PythonOperator
import sys
sys.path.append('/usr/local/Python/Python313/Lib/site-packages')
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
#sys.path.insert(0, "/usr/local/airflow/.local/lib/python3.7/site-packages")
os.environ["SPARK_HOME"] = "/usr/local/airflow/dags/spark"
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
dag = DAG(dag_id='GrowthRevenue_per_month', catchup=False, schedule_interval= "@daily", default_args=default_args)

'''#3. DEFINE FUNCTION TO EXTRACT, CLEAN & TRANSFORM DATA'''
# Define function to automatically scrape schema query and populate with artificial data
def extract_data():
    # Chrome driver to visit clickup site to scrape queries and table
    wd = webdriver.Chrome(options=chrome_options )
    wd.get('https://doc.clickup.com/3627772/p/h/3epqw-172805/3c092c33c63dada')
    # Wait for all page elements to load
    time.sleep(5)
    # FInd the particular schema in web page
    query1 = wd.find_elements(By.XPATH, "/html/body/app-root/cu-document-page/cu-dashboard-doc-container/div/div[2]/div/div/cu-dashboard-doc-main/div/div/div/cu-document-page-content/div/div/div/div/pre[1]")
    extracted_query = (' '.join([element.text for element in query1])).replace('PRIMARY KEY', "")
    print(extracted_query)
    # Parse Table name from schema
    pattern1 = r"CREATE\s+TABLE\s+(\w+)"
    table_name = re.search(pattern1, extracted_query)
    table_name = table_name.group(1)
    print(table_name)
    # Parse Columns from Schema
    pattern2 = r'\b([a-z]\w+)\b'
    matches = re.findall(pattern2, extracted_query, re.MULTILINE)
    column_names = [col for col in matches]
    print(column_names)
    # Generate random data
    num_rows = 100
    data = []
    for _ in range(num_rows):
        sale_id = random.randint(1, 1000)
        sale_date = date.today() - timedelta(days=random.randint(0, 365))
        revenue = random.uniform(100, 1000)
        data.append((sale_id, sale_date, revenue))
    # Create Spark Dataframe from random Data
    sales_df = pl.DataFrame({'sale_id': [row[0] for row in data],
                            'sale_date': [row[1] for row in data],
                            'revenue': [row[2] for row in data]
                            })
    sales_df = spark.createDataFrame(sales_df.to_pandas(), column_names)
    sales_df.show()
    sales_df.write.csv("Sales.csv", mode='Overwrite')

# Function to transform data by calculating the 
# month-over-month (MoM) growth rate of revenue (with Spark SQL)
def transform_data():
    sales_df = spark.read.csv("Sales.csv")
    ##SQL query to extract month from datetime and aggregate by month
    # Convert spark dataframe to SQL table
    sales_df.createOrReplaceTempView(table_name)
    sales_df = spark.sql('''WITH Revenue_Per_Month AS (
                SELECT
                    DATE(DATE_TRUNC('month', sale_date)) AS month_only,
                    SUM(revenue) AS revenue_sum
                FROM
                    Sales
                GROUP BY
                    DATE_TRUNC('month', sale_date)
            ),
            Revenue_previous AS (
                SELECT
                    month_only,
                    revenue_sum,
                    LAG(revenue_sum, 1) OVER (ORDER BY month_only) AS previous_month_revenue
                FROM
                    Revenue_Per_Month
            )
            SELECT
                month_only,
                revenue_sum,
                previous_month_revenue,
                (revenue_sum - previous_month_revenue) / previous_month_revenue * 100 AS growth_rate
            FROM
                Revenue_previous;;''')
    sales_df.show()
    # Save dataframe as CSV format (In big Data, use parquet instead)
    sales_df.write.csv("RevenueGrowthRate(MOM).csv", mode='Overwrite')
    print('task 1 completed')    

'''# DEFINE TASKS AND DEPENDENCIES'''
t1 = PythonOperator(task_id = 'extract_and_clean',
    python_callable=extract_data,
    dag=dag)
t2 = PythonOperator(task_id='calculate_and_trasform',
    python_callable=transform_data,
    dag=dag)

# Define dependencies
t1 >> t2 
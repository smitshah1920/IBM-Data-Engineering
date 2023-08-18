# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

# defining DAG arguments
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Sam Smith',
    'start_date': days_ago(0),
    'email': ['samsmith@xyz.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

# defining the DAG
dag = DAG(
    dag_id = 'Capstone-etl-dag',
    default_args = default_args,
    description = 'Capstone Project ETL DAG',
    schedule_interval = timedelta(days=1),
    )

# defining the first task to extract_data by calling shell script

extract_data = BashOperator(
    task_id = "extract_data",
    bash_command = 'cut -d" " -f1 /home/project/airflow/dags/accesslog.txt > /home/project/airflow/dags/extracted_data.txt',
    dag = dag,
    )

# defining the second task to transform_data 
# filter out all the occurrences of ipaddress “198.46.149.143” from extracted_data.txt and save toutput to transformed_data.txt.

transform_data = BashOperator(
    task_id = "transform_data",
    bash_command = 'grep -v "198.46.149.143" /home/project/airflow/dags/extracted_data.txt > /home/project/airflow/dags/transformed_data.txt',
    dag = dag,
    )

# defining the third task to load_data 
# archive the file 'transformed_data.txt' into a tar file named 'weblog.tar'.
load_data = BashOperator(
    task_id = "load_data",
    bash_command = 'tar -cvf /home/project/airflow/dags/weblog.tar /home/project/airflow/dags/transformed_data.txt',
    dag = dag,
    )

# task Pipeline
extract_data >> transform_data >> load_data

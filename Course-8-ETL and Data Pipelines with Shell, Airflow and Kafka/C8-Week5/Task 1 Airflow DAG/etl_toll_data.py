# importing the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# DAG argument Block (Task 1.1)
default_args = {
    'owner': 'Smit Shah',
    'start_date': days_ago(0),
    'email': ['smitshah@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Definition (Task 1.2)
dag = DAG(
    dag_id='etl_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)

# Create a task to unzip_data (Task 1.3)
unzip_data = BashOperator(
    task_id='unzip_data',
    dag=dag,
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/staging/tolldata.tgz',
)

# Create a task to extract data from csv file (Task 1.4)
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    dag=dag,
    bash_command='cut -d"," -f1-4 /home/project/airflow/dags/finalassignments/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignments/staging/csv_data.csv',
)

# Create a task to extract data from tsv file (Task 1.5)
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    dag=dag,
    bash_command='cut -f5-7 /home/project/airflow/dags/finalassignments/staging/tollplaza-data.tsv > /home/project/airflow/dags/finalassignments/staging/tsv_data.csv',
)

# Create a task to extract data from fixed width file (Task 1.6)
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    dag=dag,
    bash_command="cat payment-data.txt | tr -s '[:space:]' | cut -d'  ' -f11,12 > /home/project/airflow/dags/finalassignments/staging/fixed_width_data.csv",
)

# Create a task to consolidate data extracted from previous tasks (Task 1.7)
consolidate_data = BashOperator(
    task_id='consolidate_data',
    dag=dag,
    bash_command='paste /home/project/airflow/dags/finalassignments/staging/csv_data.csv /home/project/airflow/dags/finalassignments/staging/tsv_data.csv /home/project/airflow/dags/finalassignments/staging/fixed_width_data.csv > /home/project/airflow/dags/finalassignments/staging/extracted_data.csv',
)

# Transform and load the data (Task1.8)
transform_data = BashOperator(
    task_id='transform_data',
    dag=dag,
    bash_command='tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/finalassignments/staging/extracted_data.csv > /home/project/airflow/dags/finalassignments/staging/transformed_data.csv',
)

# Define the task pipeline (Task 1.9)
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

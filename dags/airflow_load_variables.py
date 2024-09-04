from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json
import os
import boto3

# Define the DAG
dag = DAG(
    'Airflow_load_variables',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(1),
    },
    description='A DAG to load Airflow variables from a JSON file',
    schedule_interval=None,
)

# Define the function to load variables
def load_variables_from_json():

    s3_bucket = "nbn-pipelines"  # Source bucket name - hardcode as we don't have variables yet
    s3_key = "airflow-variables-sanitised.json"  # Path to the JSON file in the bucket

    # Initialize a session using boto3
    s3_client = boto3.client('s3')

    # Fetch the JSON file from S3
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    json_content = response['Body'].read().decode('utf-8')

    # Parse the JSON content
    variables = json.loads(json_content)

    # Set Airflow Variables
    for key, value in variables.items():
        Variable.set(key, value)

# Define the PythonOperator
load_variables_task = PythonOperator(
    task_id='load_variables',
    python_callable=load_variables_from_json,
    dag=dag,
)

# Setting the task in the DAG
load_variables_task

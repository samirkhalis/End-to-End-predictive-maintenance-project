import uuid
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import logging
import requests
import time

default_args = {
    'owner': 'zahra',
    'start_date': datetime(2024, 8, 10, 10, 00)
}

def get_data():
    """Fetch random data from host machine API."""
    try:
        # Use host.docker.internal for Mac/Windows Docker
        # Use 172.17.0.1 or 172.18.0.1 for Linux Docker
        res = requests.get("http://host.docker.internal:5000/record")
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from host API: {e}")
        return None

import uuid

def format_data(res):
    """Format the fetched data for the given structure."""
    if not res:
        return None

    data = {}
    data['id'] = str(uuid.uuid4())  # Generate unique ID

    # Map the input data to a structured format
    data['air_temperature'] = res.get('Air temperature [K]', None)
    data['failure_type'] = res.get('Failure Type', 'Unknown')
    data['process_temperature'] = res.get('Process temperature [K]', None)
    data['product_id'] = res.get('Product ID', 'Unknown')
    data['rotational_speed'] = res.get('Rotational speed [rpm]', None)
    data['target'] = res.get('Target', None)
    data['tool_wear'] = res.get('Tool wear [min]', None)
    data['torque'] = res.get('Torque [Nm]', None)
    data['type'] = res.get('Type', 'Unknown')
    data['udi'] = res.get('﻿UDI', None)  # Note: Check for hidden characters in key names if issues occur

    return data


def stream_data(formatted_data):
    """Stream the formatted data to Kafka."""
    if not formatted_data:
        logging.error("No data to stream")
        return
    
    try:
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
        curr_time = time.time()

        while True:
            if time.time() > curr_time + 20:  # Run for 20 seconds
                break
            try:
                # Get and format new data in each iteration
                res = get_data()
                if res:
                    formatted_data = format_data(res)
                    producer.send('users_created', json.dumps(formatted_data).encode('utf-8'))
            except Exception as e:
                logging.error(f'An error occurred: {e}')
                continue
    except Exception as e:
        logging.error(f"Kafka producer error: {e}")

# Define the DAG
with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # Task 1: Get data from the API
    get_data_task = PythonOperator(
        task_id='get_data_task',
        python_callable=get_data
    )

    # Task 2: Format the fetched data
    format_data_task = PythonOperator(
        task_id='format_data_task',
        python_callable=format_data,
        op_args=[get_data_task.output],
    )

    # Task 3: Stream the formatted data to Kafka
    stream_data_task = PythonOperator(
        task_id='stream_data_task',
        python_callable=stream_data,
        op_args=[format_data_task.output],
    )

    # Task dependencies
    get_data_task >> format_data_task >> stream_data_task
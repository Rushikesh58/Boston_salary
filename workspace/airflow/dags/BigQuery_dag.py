from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import csv
import json
import pandas as pd
import numpy as np
from pandas_gbq import to_gbq


def rest_api():
    def apitest(url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            res = response.json()
            return res
        except requests.exceptions.HTTPError as errh:
            print(f"HTTP Error occurred: {errh}")
        except requests.exceptions.ConnectionError as errc:
            print(f"Error connecting: {errc}")
        except requests.exceptions.Timeout as errt:
            print(f"Timeout Error: {errt}")
        except requests.exceptions.RequestException as err:
            print(f"Request Exception occurred: {err}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


    def api(url):
        #get call to REST API
        #response = requests.get(url)
        #res = response.json()
        res = apitest(url)
        #traversing through json to gether required information
        crime_data = res['result']['records']
        #headers: all the columns present within the dataset
        headers = list(crime_data[0].keys())
        #writing the json file to data.csv file
        with open('data.csv', 'w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames = headers)
            writer.writeheader()
            for row in crime_data:
                writer.writerow(row)
        crime = pd.read_csv('data.csv', header=0)
        return crime

    URL = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=63ac638b-36c4-487d-9453-1d83eb5090d2&limit=29000'
    sal = api(URL)
    return(sal)

dag = DAG(
    'BigQuery_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 0 1 * *',
    catchup=False
)

Fetch_data_API = PythonOperator(
    task_id='Fetch_data_API',
    python_callable=rest_api,
    dag=dag
)

def pushcode_to_bq(input_value, **kwargs):
    project_id = 'bostonsalary'
    dataset_name = 'BostonSalary'
    table_name = 'SalaryData'

    destination_table = f"{project_id}.{dataset_name}.{table_name}"

    # Use the to_gbq function to upload the DataFrame to BigQuery
    to_gbq(input_value, destination_table, project_id=project_id, if_exists='replace')
    print(f'DataFrame successfully uploaded to BigQuery table: {destination_table}')

pushcode_bq = PythonOperator(
    task_id='pushcode_bq',
    python_callable=pushcode_to_bq,
    op_args=[Fetch_data_API.output],
    dag=dag
)

# Set the dependencies between the tasks
Fetch_data_API >> pushcode_bq
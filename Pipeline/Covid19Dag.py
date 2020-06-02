import csv
import datetime
import json
import os
from datetime import timedelta
from airflow.utils.dates import days_ago
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = "/home/nineleaps/PycharmProjects/COVID19_Airflow/Pipeline/COVID19AIRFLOW.json"
dataset_id = 'covid_state_data'
table_id = 'states_data'
client = bigquery.Client()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(dag_id='covid',
          default_args=default_args,
          description="DAG to fetch and upload covid data in big query",
          schedule_interval=timedelta(days=1),
          )


def fetch_covid_state_data():
    req = requests.get('https://api.covidindiatracker.com/state_data.json')
    url_data = req.text
    data = json.loads(url_data)
    covid_data = [['date', 'state', 'number_of_cases']]
    date = datetime.datetime.today().strftime('%Y-%m-%d')

    for state in data:
        covid_data.append([date, state.get('state'), state.get('aChanges')])

    with open("/home/nineleaps/PycharmProjects/COVID19_Airflow/Pipeline/covid_data/covid_data_{}.csv".format(date),
              "w") as f:
        writer = csv.writer(f)
        writer.writerows(covid_data)


def upload_covid_data():
    filename = "/home/nineleaps/PycharmProjects/COVID19_Airflow/Pipeline/covid_data/covid_data_{}.csv".format(
        datetime.datetime.today().strftime('%Y-%m-%d'))
    try:
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True

        with open(filename, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()  # Waits for table load to complete.

        print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
        return job.output_rows
    except Exception as e:
        print(e)


def percent_upload(**kwargs):
    query = """
             select count(*) as total_rows from {}.{};
            """.format(dataset_id, table_id)
    total_rows = 0
    query_job = client.query(query)
    for row in query_job:
        total_rows = row[0]
    rows_affected = kwargs['ti'].xcom_pull(task_ids=['upload_covid_data'])
    print("Percentage upload of data: {}".format((total_rows / rows_affected[0]) * 100))


t1 = PythonOperator(task_id='fetch_data', python_callable=fetch_covid_state_data, dag=dag)

t2 = PythonOperator(task_id='upload_covid_data', python_callable=upload_covid_data, dag=dag)

t3 = PythonOperator(task_id='percent_upload', python_callable=percent_upload, provide_context=True, dag=dag)

t1 >> t2 >> t3

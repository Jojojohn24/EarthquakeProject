from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

# DAG settings
default_args = {
    'owner': 'airflow',
    'start_date':days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


with DAG(
    dag_id='daily_earthquake_ingestion',
    default_args=default_args,
    description='daily ingestion of  earthquake data into BigQuery',
    schedule_interval='@daily',
    catchup=False,
) as dag:


    dataproc_job = {
        'reference': {'project_id':  "seismic-operand-441006-c9"},
        'placement': {'cluster_name': 'cluster-99bd'},
        'pyspark_job': {
            'main_python_file_uri': 'gs://earthquake97/daily_pyspark/daily_pycharm.py',
            'python_file_uris': ['gs://earthquake97/daily_pyspark/Utils.py'],
            'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar'],
        },
    }


    submit_dataproc_job = DataprocSubmitJobOperator(
        task_id='submit_daily_dataproc_job',
        job=dataproc_job,
        region='us-central1',
        project_id='seismic-operand-441006-c9',
        gcp_conn_id='earthquake_conn',

    )

    submit_dataproc_job
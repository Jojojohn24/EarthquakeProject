import os

from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.utils.dates import days_ago



# Set up GCP credentials and configurations from config file
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gs://earthquake97/dataflow/airflow/seismic-operand-441006-c9-eb218210584d.json'
# DAG settings
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG(
    dag_id='daily_earthquake_ingestion',
    default_args=default_args,
    description='Ingest earthquake data daily to Dataflow',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    # Define the Dataflow operator to run the Beam pipeline


    dataflow_operator = DataflowCreatePythonJobOperator(
        task_id='run_dataflow_pipeline',
        job_name='beam-dataflow-job',
        py_file='gs://earthquake97/dataflow/airflow/daily_df.py',# Path to the pipeline on GCS
        location='us-central1',  # Choose the appropriate region for Dataflow
        gcp_conn_id='daily_dataflow',
        options={
            'project': "seismic-operand-441006-c9",
            'region': 'us-central1',
            'temp_location': f"gs://earthquake97/dataflow/temp_location1",  # Temporary location in GCS for staging
            'staging_location': f"gs://earthquake97/dataflow/stage_location1",  # Staging location
            'runner': 'DataflowRunner',
            'job_name': 'dataflow-beam-job',
            'input': 'gs://earthquake97/dataflow/input/',
            'output': 'gs://earthquake97/dataflow/output/',

        }
    )

    # Define task dependencies (you can add more tasks if necessary)
    dataflow_operator

from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.utils.dates import days_ago

# Define the utility files you want to include
utility_files = [
    'gs://earthquake97/dataflow/airflow/config.py',  # Cloud Storage path for config.py
    'gs://earthquake97/dataflow/airflow/utils_df.py'  # Cloud Storage path for utils_df.py
]

# Define the main DAG and Dataflow operator
with models.DAG(
        'dataflow_with_manually_staged_utility_files',
        schedule_interval=None,  # Specify a schedule or leave as None for manual execution
        start_date=days_ago(1),
) as dag:
    # Define the Dataflow job operator
    dataflow_python_operator = DataflowCreatePythonJobOperator(
        task_id='dataflow_python_operator',

        # Path to the main pipeline script in Google Cloud Storage
        py_file='gs://earthquake97/dataflow/test2.py',

        # Set options for the Dataflow job
        job_name='your-job-name',
        location='us-central1',
        gcp_conn_id='daily_dataflow',
        options={
            'project': "seismic-operand-441006-c9",
            'region': 'us-central1',
            'temp_location': 'gs://earthquake97/dataflow/temp_location1',
            'staging_location': 'gs://earthquake97/dataflow/stage_location1',
            'runner': 'DataflowRunner',
            'input': 'gs://earthquake97/dataflow/input/',
            'output': 'gs://earthquake97/dataflow/output/',
            # Include paths to the utility files here
            'extra_packages': 'gs://earthquake97/dataflow/dependencies.zip',  # If you're using extra packages
        },

        # Manually include the utility files by passing them via py_files
        py_files=utility_files  # This is the correct argument
    )

    # Execute the operator
    dataflow_python_operator

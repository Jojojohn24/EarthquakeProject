import os
import logging
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions,SetupOptions
import apache_beam as beam
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.io.parquetio import ReadFromParquet
import config
from utils_df import FetchAndUpload, FlattenPlusTransformations, ProcessForBigQuery,parquet_schema,schema

import sys
# sys.path.append('/path/to/dependencies')  # Replace with path if running locally for testing
sys.path.append('gs://earthquake97/dataflow/dependencies.zip')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set up GCP credentials and configurations


TABLE_REF = f'{config.project_id}.{config.dataset_id}.{config.TABLE_ID_df}'

current_datetime_for_job_name = datetime.now().strftime('%y%m%d-%H%M%S')




# Set up GCP credentials and configurations from config file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.SERVICE_ACCOUNT_KEY

# Set up pipeline options
options = PipelineOptions(save_main_session=True, temp_location=config.TEMP_LOCATION)
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = config.project_id
job_name = f"dataflow-earthquake-{current_datetime_for_job_name}"
google_cloud_options.job_name = job_name
google_cloud_options.region = 'us-central1'
google_cloud_options.staging_location = config.DATAFLOW_STAGING_LOCATION
google_cloud_options.temp_location = config.TEMP_LOCATION

setup_options = options.view_as(SetupOptions)

# Add the extra package
# setup_options.extra_packages = ['dependencies.zip']
setup_options.extra_packages = ['gs://earthquake97/dataflow/dependencies.zip']


# Set worker configurations
options.view_as(StandardOptions).runner = 'DataflowRunner'


def start_pipeline():
    with beam.Pipeline(options=options) as p:
        # Step 1: Fetch and upload raw JSON data to GCS
        _ = (
                p
                | 'Create Single Element' >> beam.Create([None])  # Start trigger
                | 'Fetch and Upload Data' >> beam.ParDo(
            FetchAndUpload(config.daily_url, config.bucket_name, config.DATAFLOW_LANDING_LOCATION1))
        )


def pipeline_for_transformations():
    with beam.Pipeline(options=options) as p:
        trigger_and_data = (
            p
            | 'Wait for Upload Completion' >> beam.Create([None])
            | 'Read JSON from GCS' >> beam.io.ReadFromText(
                f"gs://{config.bucket_name}/{config.DATAFLOW_LANDING_LOCATION1}")
            | 'Parse and Flatten Data' >> beam.ParDo(FlattenPlusTransformations()).with_outputs('bq', 'silver')
        )

        # Access each tagged output separately
        bq_data = trigger_and_data.bq
        silver_data = trigger_and_data.silver

        # Optionally print for debugging
        bq_data | 'Print BQ Data' >> beam.Map(print)
        silver_data | 'Print Silver Data' >> beam.Map(print)

        # Write silver layer to Parquet
        silver_data | 'Write Transformed Data to Parquet (Silver Layer)' >> WriteToParquet(
            file_path_prefix=f'gs://{config.bucket_name}/{config.DATAFLOW_SILVER_LAYER_PATH1}',
            schema=parquet_schema,
            file_name_suffix='.parquet',
            num_shards=1
        )


def pipeline_for_bigquery():
    with beam.Pipeline(options=options) as p:
        # Read back the Parquet file from GCS Silver Layer
        read_parquet = (
                p
                | 'Read From Parquet' >> ReadFromParquet(
            f'gs://{config.bucket_name}/{config.DATAFLOW_SILVER_LAYER_PATH1}-00000-of-00001.parquet')
        # gs://production-bucket-dipakraj/dataflow_silver/20241019/TESTING1-00000-of-00001.parquet
        )
        # Add new ParDo for processing before BigQuery upload
        processed_for_bq = read_parquet | 'Process For BigQuery' >> beam.ParDo(ProcessForBigQuery())

        # Write to BigQuery as before
        processed_for_bq | 'Write Transformed to BigQuery' >> beam.io.WriteToBigQuery(
            TABLE_REF,
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':

    start_pipeline()
    pipeline_for_transformations()
    pipeline_for_bigquery()

logging.info("Pipelines executed successfully.")

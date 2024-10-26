import json
import requests
import apache_beam as beam
from google.cloud import storage
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import os
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, ArrayType
from pyspark.sql import SparkSession


class FetchData(beam.DoFn):
    def process(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            yield response.json()
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching data: {e}")


class UploadToGCS(beam.DoFn):
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def process(self, data):
        # Create the GCS client here to avoid pickling issues
        client = storage.Client()
        date_str = datetime.now().strftime('%Y%m%d')
        blob_name = f'bronze/dataflow/landing/{date_str}/raw_data.json'
        bucket = client.bucket(self.bucket_name)
        blob = bucket.blob(blob_name)

        # Upload the data
        blob.upload_from_string(data=json.dumps(data), content_type='application/json')
        print(f"Upload of {blob_name} complete.")
        yield f"Uploaded to {blob_name}"


def run_UploadToGCS(url, bucket_name):
    options = PipelineOptions()

    with beam.Pipeline(options=options) as pipeline:
        (pipeline
         | 'Create URL' >> beam.Create([url])
         | 'Fetch Data' >> beam.ParDo(FetchData())
         | 'Upload to GCS' >> beam.ParDo(UploadToGCS(bucket_name)))
class DownloadFromGCS(beam.DoFn):
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def process(self, _):
        # Create the GCS client inside the process method to avoid pickling issues
        client = storage.Client()
        date_str = datetime.now().strftime('%Y%m%d')
        source_file_name = f'bronze/dataflow/landing/{date_str}/raw_data.json'  # The file to download
        bucket = client.bucket(self.bucket_name)
        blob = bucket.blob(source_file_name)

        try:
            source_data = blob.download_as_text()
            yield source_data  # Yield the downloaded text
        except Exception as e:
            print(f"An error occurred while downloading data: {e}")

def run_download_from_gcs(bucket_name):
    options = PipelineOptions()

    with beam.Pipeline(options=options) as pipeline:
        (pipeline
         | 'Download from GCS' >> beam.Create([None])  # Create a single element to trigger processing
         | 'Read from GCS' >> beam.ParDo(DownloadFromGCS(bucket_name))
         | 'Print Data' >> beam.Map(print))


def create_dataframe_from_json(json_data):
    # Define the schema
    schema = StructType([
        StructField("type", StringType(), True),
        StructField("metadata", StructType([
            StructField("generated", LongType(), True),
            StructField("url", StringType(), True),
            StructField("title", StringType(), True),
            StructField("status", IntegerType(), True),
            StructField("api", StringType(), True),
            StructField("count", IntegerType(), True)
        ]), True),
        StructField("features", ArrayType(StructType([
            StructField("type", StringType(), True),
            StructField("properties", StructType([
                StructField("mag", FloatType(), True),
                StructField("place", StringType(), True),
                StructField("time", StringType(), True),
                StructField("updated", StringType(), True),
                StructField("tz", IntegerType(), True),
                StructField("url", StringType(), True),
                StructField("detail", StringType(), True),
                StructField("felt", IntegerType(), True),
                StructField("cdi", FloatType(), True),
                StructField("mmi", FloatType(), True),
                StructField("alert", StringType(), True),
                StructField("status", StringType(), True),
                StructField("tsunami", IntegerType(), True),
                StructField("sig", IntegerType(), True),
                StructField("net", StringType(), True),
                StructField("code", StringType(), True),
                StructField("ids", StringType(), True),
                StructField("sources", StringType(), True),
                StructField("types", StringType(), True),
                StructField("nst", IntegerType(), True),
                StructField("dmin", FloatType(), True),
                StructField("rms", FloatType(), True),
                StructField("gap", FloatType(), True),
                StructField("magType", StringType(), True),
                StructField("title", StringType(), True)
            ]), True),
            StructField("geometry", StructType([
                StructField("type", StringType(), True),
                StructField("coordinates", ArrayType(FloatType()), True)
            ]), True),
            StructField("id", StringType(), True)
        ])), True)
    ])

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Load DataFrame from GCS") \
        .getOrCreate()

    # Convert JSON data to DataFrame
    df = spark.read.json(spark.sparkContext.parallelize([json_data]), schema=schema)
    return df


def run_create_df_from_json(bucket_name):
    options = PipelineOptions()

    with beam.Pipeline(options=options) as pipeline:
        downloaded_data = (
            pipeline
            | 'Download from GCS' >> beam.Create([None])  # Create a single element to trigger processing
            | 'Read from GCS' >> beam.ParDo(DownloadFromGCS(bucket_name))
            | 'Collect Data' >> beam.combiners.ToList()  # Collect all data into a single list
        )

        # Convert the PCollection to a DataFrame after the pipeline
        collected_data = downloaded_data | beam.Map(lambda data: create_dataframe_from_json(data[0]) if data else None)


if __name__ == '__main__':
    # Initialization of Google application
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'F:\pyspark\gcp_session\session\gcp-2024-431610-6d2aaa851b30.json'

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = "gcp-2024-431610"
    google_cloud_options.job_name = "simple-dataflow-job"
    google_cloud_options.region = "us-central1"
    google_cloud_options.staging_location = "gs://earthquake_analysis_pallavi/dataflow/stage_location1"
    google_cloud_options.temp_location = "gs://earthquake_analysis_pallavi/dataflow/temp_location1"

    url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'
    bucket_name = "earthquake_analysis_pallavi"

    run_UploadToGCS(url, bucket_name)
    run_download_from_gcs(bucket_name)
    # run_create_df_from_json(bucket_name)
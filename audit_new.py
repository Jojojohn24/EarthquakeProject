from google.cloud import storage, bigquery

import requests
import json
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, LongType, IntegerType, ArrayType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_unixtime, regexp_extract, current_timestamp
from google.cloud import bigquery
from datetime import datetime
import config






def log_audit_event(job_id, pipeline_name, function_name, start_time, end_time, status, error_msg, process_record):
    client = bigquery.Client()
    table_id = f"{config.project_id}.{config.dataset_id}.pyspark_audit_table"  # Replace with actual project and dataset details

    # Convert datetime objects to string format
    start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(start_time, datetime) else start_time
    end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(end_time, datetime) else end_time

    rows_to_insert = [
        {
            "job_id": job_id,
            "pipeline_name": pipeline_name,
            "function_name": function_name,
            "start_time": start_time_str,
            "end_time": end_time_str,
            "status": status,
            "error_msg": error_msg,
            "process_record": process_record
        }
    ]

    # Try to insert rows and handle any insertion errors
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        # Append insertion errors to the error_msg column and log them
        error_details = "; ".join([str(error) for error in errors])
        rows_to_insert[0]["error_msg"] = f"{error_msg}; Insertion Error: {error_details}"

        # Retry logging with updated error message
        retry_errors = client.insert_rows_json(table_id, rows_to_insert)
        if retry_errors:
            print("Further errors occurred while inserting audit record:", retry_errors)
        else:
            print("Audit record with insertion errors logged successfully.")
    else:
        print("Audit record inserted successfully.")



class DataFetcher:
    @staticmethod
    def fetch_data_from_url(url):
        """
        Fetches JSON data from the specified URL.
        """
        start_time = datetime.utcnow()
        try:
            response = requests.get(url)
            if response.status_code == 200:
                end_time = datetime.utcnow()
                log_audit_event(config.job_id, "DataPipeline", "fetch_data_from_url", start_time, end_time, "Success", None, 1)
                return response.json()  # Return the JSON data
            else:
                end_time = datetime.utcnow()
                log_audit_event(config.job_id, "DataPipeline", "fetch_data_from_url", start_time, end_time, "Failed", f"Status code: {response.status_code}", 0)
                print(f"Failed to fetch data. Status code: {response.status_code}")
                return None
        except Exception as e:
            end_time = datetime.utcnow()
            log_audit_event(config.job_id, "DataPipeline", "fetch_data_from_url", start_time, end_time, "Failed", str(e), 0)
            print(f"Exception occurred: {e}")
            return None


class CloudStorageUploader:
    def __init__(self, bucket_name):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def upload_data_to_bucket_bronze_layer(self, url):
        """
        Uploads JSON data from URL to the specified Google Cloud Storage bucket.
        """
        start_time = datetime.utcnow()
        data = DataFetcher.fetch_data_from_url(url)
        if data is not None:
            try:
                date_str = datetime.now().strftime('%Y%m%d')
                blob = self.bucket.blob(f'bronze/pyspark/landing1/{date_str}/raw_data.json')
                blob.upload_from_string(data=json.dumps(data), content_type='application/json')
                end_time = datetime.utcnow()
                log_audit_event(config.job_id, "DataPipeline", "upload_data_to_bucket_bronze_layer", start_time, end_time, "Success", None, 1)
                print("Upload of bronze data complete.")
                return True
            except Exception as e:
                end_time = datetime.utcnow()
                log_audit_event(config.job_id, "DataPipeline", "upload_data_to_bucket_bronze_layer", start_time, end_time, "Failed", str(e), 0)
                print(f"Error during upload: {e}")
                return False
        else:
            end_time = datetime.utcnow()
            log_audit_event(config.job_id, "DataPipeline", "upload_data_to_bucket_bronze_layer", start_time, end_time, "Failed", "No data to upload", 0)
            print("No data to upload.")
            return False

    def download_as_text_bronze_gcs(self):
        """
        Downloads text data from the specified path in GCS.
        """
        start_time = datetime.utcnow()
        try:
            date_str = datetime.now().strftime('%Y%m%d')
            source_file_name = f'bronze/pyspark/landing1/{date_str}/raw_data.json'
            blob = self.bucket.blob(source_file_name)
            data = blob.download_as_text()
            end_time = datetime.utcnow()
            log_audit_event(config.job_id, "DataPipeline", "download_as_text_bronze_gcs", start_time, end_time, "Success", None, 1)
            return data
        except Exception as e:
            end_time = datetime.utcnow()
            log_audit_event(config.job_id, "DataPipeline", "download_as_text_bronze_gcs", start_time, end_time, "Failed", str(e), 0)
            print(f"Error downloading from GCS: {e}")
            return None


class SparkDataProcessor:

    def __init__(self, app_name="DataPipelineSparkApp"):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    def create_dataframe(self, bucket_name):
        """
        Creates a Spark DataFrame from JSON data in GCS.
        """
        start_time = datetime.utcnow()
        try:
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
            date_str = datetime.now().strftime('%Y%m%d')
            source_blob_filepath = f'gs://{bucket_name}/bronze/pyspark/landing1/{date_str}/raw_data.json'
            df = self.spark.read.json(source_blob_filepath, schema=schema)
            end_time = datetime.utcnow()
            log_audit_event(config.job_id, "DataPipeline", "create_dataframe", start_time, end_time, "Success", None, df.count())
            return df
        except Exception as e:
            end_time = datetime.utcnow()
            log_audit_event(config.job_id, "DataPipeline", "create_dataframe", start_time, end_time, "Failed", str(e), 0)
            print(f"Error creating DataFrame: {e}")
            return None

    def flatten_df(self, bucket_name):
        """
        Flattens a nested DataFrame by extracting fields and creating separate columns for coordinates.
        """
        start_time = datetime.utcnow()
        try:
            df = self.create_dataframe(bucket_name)
            flattened_df = df.select(explode("features").alias("feature")) \
                .select(col("feature.properties.mag").cast("float").alias("mag"),
                        col("feature.properties.place").alias("place"),
                        col("feature.properties.time").alias("time"),
                        col("feature.properties.updated").alias("updated"),
                        col("feature.properties.tz").cast("int").alias("tz"),
                        col("feature.properties.url").alias("url"),
                        col("feature.properties.detail").alias("detail"),
                        col("feature.properties.felt").cast("int").alias("felt"),
                        col("feature.properties.cdi").cast("float").alias("cdi"),
                        col("feature.properties.mmi").cast("float").alias("mmi"),
                        col("feature.properties.alert").alias("alert"),
                        col("feature.properties.status").alias("status"),
                        col("feature.properties.tsunami").cast("int").alias("tsunami"),
                        col("feature.properties.sig").cast("int").alias("sig"),
                        col("feature.properties.net").alias("net"),
                        col("feature.properties.code").alias("code"),
                        col("feature.properties.ids").alias("ids"),
                        col("feature.properties.sources").alias("sources"),
                        col("feature.properties.types").alias("types"),
                        col("feature.properties.nst").cast("int").alias("nst"),
                        col("feature.properties.dmin").cast("float").alias("dmin"),
                        col("feature.properties.rms").cast("float").alias("rms"),
                        col("feature.properties.gap").cast("float").alias("gap"),
                        col("feature.properties.magType").alias("magType"),
                        col("feature.properties.title").alias("title"),
                        col("feature.geometry.coordinates").getItem(0).alias("longitude"),
                        col("feature.geometry.coordinates").getItem(1).alias("latitude"),
                        col("feature.geometry.coordinates").getItem(2).alias("depth"),
                        col("feature.id").alias("id"),
                        current_timestamp().alias("ingestion_timestamp"))
            end_time = datetime.utcnow()
            log_audit_event(config.job_id, "DataPipeline", "flatten_df", start_time, end_time, "Success", None, flattened_df.count())
            return flattened_df
        except Exception as e:
            end_time = datetime.utcnow()
            log_audit_event(config.job_id, "DataPipeline", "flatten_df", start_time, end_time, "Failed", str(e), 0)
            print(f"Error flattening DataFrame: {e}")
            return None

    def transform_columns(self, bucket_name):
        """
        Transforms the DataFrame by converting epoch time to timestamp, extracting area from place, and adding a current timestamp.
        """
        start_time = datetime.utcnow()
        try:
            flattened_df = self.flatten_df(bucket_name)
            transformed_df = flattened_df.withColumn("time", from_unixtime(col("time") / 1000).cast("timestamp")) \
                                         .withColumn("updated", from_unixtime(col("updated") / 1000).cast("timestamp")) \
                                         .withColumn("area", regexp_extract(col("place"), r"of (.+)$", 1)) \
                                         .withColumn("insert_dt", current_timestamp())
            end_time = datetime.utcnow()
            log_audit_event(config.job_id, "DataPipeline", "transform_columns", start_time, end_time, "Success", None, transformed_df.count())
            return transformed_df
        except Exception as e:
            end_time = datetime.utcnow()
            log_audit_event(config.job_id, "DataPipeline", "transform_columns", start_time, end_time, "Failed", str(e), 0)
            print(f"Error transforming columns: {e}")
            return None

    def save_to_gcs(self, bucket_name, layer="silver"):
        """
        Saves the transformed DataFrame to GCS in Parquet format.
        """
        start_time = datetime.utcnow()
        try:
            transformed_df = self.transform_columns(bucket_name)
            if transformed_df is None:
                raise ValueError("Transformed DataFrame is None")

            date_str = datetime.now().strftime('%Y%m%d')
            gcs_path = f"gs://{bucket_name}/{layer}/pyspark/landing1/{date_str}/transformed_data"
            transformed_df.write.parquet(gcs_path, mode='overwrite')
            end_time = datetime.utcnow()
            log_audit_event(config.job_id, "DataPipeline", "save_to_gcs", start_time, end_time, "Success", None, transformed_df.count())
            print("Data saved to GCS successfully.")
            return gcs_path
        except Exception as e:
            end_time = datetime.utcnow()
            log_audit_event(config.job_id, "DataPipeline", "save_to_gcs", start_time, end_time, "Failed", str(e), 0)
            print(f"Error saving to GCS: {e}")
            return None


class BigQueryLoader:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)

    def load_parquet_to_bigquery(self, bucket_name, dataset_id, table_id):
        """
        Loads a Parquet file from GCS into a BigQuery table, creating the dataset if it doesn't exist.
        """
        start_time = datetime.utcnow()
        date_str = datetime.now().strftime('%Y%m%d')
        gcs_path = f"gs://{bucket_name}/silver/pyspark/landing1/{date_str}/transformed_data/*.parquet"

        schema = [
            {"name": "mag", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "place", "type": "STRING", "mode": "NULLABLE"},
            {"name": "time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "tz", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "detail", "type": "STRING", "mode": "NULLABLE"},
            {"name": "felt", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "cdi", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "mmi", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "alert", "type": "STRING", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tsunami", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "sig", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "net", "type": "STRING", "mode": "NULLABLE"},
            {"name": "code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ids", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sources", "type": "STRING", "mode": "NULLABLE"},
            {"name": "types", "type": "STRING", "mode": "NULLABLE"},
            {"name": "nst", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "dmin", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rms", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "gap", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "magType", "type": "STRING", "mode": "NULLABLE"},
            {"name": "title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "geometry_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "depth", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "area", "type": "STRING", "mode": "NULLABLE"},
            {"name": "insert_dt", "type": "TIMESTAMP", "mode": "NULLABLE"}
        ]

        dataset_ref = f"{self.client.project}.{dataset_id}"

        try:
            self.client.get_dataset(dataset_ref)
        except:
            self.client.create_dataset(dataset_ref)

        table_ref = self.client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField.from_api_repr(field) for field in schema],
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        try:
            load_job = self.client.load_table_from_uri(gcs_path, table_ref, job_config=job_config)
            load_job.result()
            end_time = datetime.utcnow()
            log_audit_event(config.job_id, "DataPipeline", "load_parquet_to_bigquery", start_time, end_time, "Success", None, load_job.output_rows)
            print(f"Data loaded successfully into {dataset_id}.{table_id}.")
        except Exception as e:
            end_time = datetime.utcnow()
            log_audit_event(config.job_id, "DataPipeline", "load_parquet_to_bigquery", start_time, end_time, "Failed", str(e), 0)
            print(f"Error loading data to BigQuery: {e}")

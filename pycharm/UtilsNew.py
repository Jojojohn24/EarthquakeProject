import requests
import json
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, LongType, IntegerType, ArrayType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_unixtime, regexp_extract, current_timestamp
from google.cloud import storage, bigquery
from datetime import datetime


class DataFetcher:
    @staticmethod
    def fetch_data_from_url(url):
        """
        Fetches JSON data from the specified URL.
        """
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()  # Return the JSON data
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return None


class CloudStorageUploader:
    def __init__(self, bucket_name):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def upload_data_to_bucket_bronze_layer(self, url):
        """
        Uploads JSON data from URL to the specified Google Cloud Storage bucket.
        """
        data = DataFetcher.fetch_data_from_url(url)
        if data is not None:
            date_str = datetime.now().strftime('%Y%m%d')
            blob = self.bucket.blob(f'bronze/pyspark/landing/{date_str}/raw_data.json')
            blob.upload_from_string(data=json.dumps(data), content_type='application/json')
            print("Upload of bronze data complete.")
            return True
        else:
            print("No data to upload.")
            return False

    def download_as_text_bronze_gcs(self):
        """
        Downloads text data from the specified path in GCS.
        """
        date_str = datetime.now().strftime('%Y%m%d')
        source_file_name = f'bronze/pyspark/landing/{date_str}/raw_data.json'
        blob = self.bucket.blob(source_file_name)
        return blob.download_as_text()


class SparkDataProcessor:
    def __init__(self, app_name):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    def create_dataframe(self, bucket_name):
        """
        Creates a Spark DataFrame from JSON data in GCS.
        """
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
        source_blob_filepath = f'gs://{bucket_name}/bronze/pyspark/landing/{date_str}/raw_data.json'
        return self.spark.read.json(source_blob_filepath, schema=schema)

    def flatten_df(self, bucket_name):
        """
        Flattens a nested DataFrame by extracting fields and creating separate columns for coordinates.
        """
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
                    col("feature.geometry.type").alias("geometry_type"),
                    col("feature.geometry.coordinates").alias("coordinates")
                    )

        flattened_df = flattened_df.withColumn("longitude", flattened_df["coordinates"].getItem(0)) \
            .withColumn("latitude", flattened_df["coordinates"].getItem(1)) \
            .withColumn("depth", flattened_df["coordinates"].getItem(2)) \
            .drop("coordinates")

        return flattened_df

    def transform_columns(self, bucket_name):
        """
        Transforms the DataFrame by converting epoch time to timestamp, extracting area from place, and adding a current timestamp.
        """
        flattened_df = self.flatten_df(bucket_name)
        return flattened_df.withColumn("time", from_unixtime(col("time") / 1000).cast("timestamp")) \
            .withColumn("updated", from_unixtime(col("updated") / 1000).cast("timestamp")) \
            .withColumn("area", regexp_extract(col("place"), r"of (.+)$", 1)) \
            .withColumn("insert_dt", current_timestamp())

    def save_to_gcs(self, bucket_name, layer="silver"):
        """
        Saves the transformed DataFrame to GCS in Parquet format.
        """
        transformed_df = self.transform_columns(bucket_name)
        date_str = datetime.now().strftime('%Y%m%d')
        gcs_path = f"gs://{bucket_name}/{layer}/pyspark/landing/{date_str}/transformed_data"
        transformed_df.write.parquet(gcs_path, mode='overwrite')
        return gcs_path


class BigQueryLoader:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)

    def load_parquet_to_bigquery(self, bucket_name, dataset_id, table_id):
        """
        Loads a Parquet file from GCS into a BigQuery table, creating the dataset if it doesn't exist.
        """
        date_str = datetime.now().strftime('%Y%m%d')
        gcs_path = f"gs://{bucket_name}/silver/pyspark/landing/{date_str}/transformed_data/*.parquet"

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

        load_job = self.client.load_table_from_uri(gcs_path, table_ref, job_config=job_config)
        load_job.result()
        print(f"Data loaded successfully into {dataset_id}.{table_id}.")

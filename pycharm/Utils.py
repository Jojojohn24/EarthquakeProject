import requests
import json
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, LongType, IntegerType, ArrayType, \
    FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_unixtime, regexp_extract, current_timestamp, date_format
import os
from google.cloud import storage, bigquery
from datetime import datetime


if __name__ == '__main__':


    a = 100

def fetch_data_from_url(url):
    """
        Fetches JSON data from the specified URL.
        Args:
            url (str): The URL from which to fetch the JSON data.
        Returns:
            dict or None: The JSON data as a dictionary if the request is successful (HTTP status code 200),
                          otherwise returns None.
        Raises:
            requests.exceptions.RequestException: If the request to the URL fails due to network issues
                                                  or other request-related errors.
    """
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()  # Return the JSON data
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None


def upload_data_to_bucket_bronze_layer(url, bucket_name):
    data = fetch_data_from_url(url)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    if data is not None:
        date_str = datetime.now().strftime('%Y%m%d')
        filename = f"pyspark/landing/{date_str}/raw_data"
        blob = bucket.blob(f'bronze/pyspark/landing/{date_str}/raw_data.json')
        blob.upload_from_string(data=json.dumps(data), content_type='application/json')
        print(f"Upload of bronze/{filename} complete.")
        return True  # Indicate success
    else:
        print("No data to upload.")
        return False


def download_as_text_bronze_gcs(bucket_name):
    client = storage.Client()
    date_str = datetime.now().strftime('%Y%m%d')
    source_file_name = f'bronze/pyspark/landing/{date_str}/raw_data.json'  # The file that was uploaded earlier
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_file_name)
    source_data = blob.download_as_text()
    return source_data


def create_spark_session(app_name):
    return SparkSession.builder.appName('dailydata') \
            .getOrCreate()


def CreateDataFrame(bucket_name):
    """
        Creates a Spark DataFrame from a JSON file stored at the specified location.
        Args:
            source_blob_filepath (str): The file path or URL of the JSON file to be read into a DataFrame.
            app_name (str): The name of the Spark application.
        Returns:
            pyspark.sql.dataframe.DataFrame: A DataFrame containing the data structured according to the specified schema.
        Raises:
            pyspark.sql.utils.AnalysisException: If there is an issue with reading the JSON file.
            pyspark.sql.utils.StreamingQueryException: If there is an error in the streaming query (if applicable).
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
    spark = create_spark_session(f'creating_bucket_{bucket_name}')
    date_str = datetime.now().strftime('%Y%m%d')
    source_blob_filepath = f'gs://{bucket_name}/bronze/pyspark/landing/{date_str}/raw_data.json'
    df = spark.read.json(source_blob_filepath, schema=schema)
    return df


def FlattenDF(bucket_name):
    """
        Flattens a nested DataFrame created from a JSON file.
        This function reads a JSON file from the specified path, extracts the
        features, and flattens the DataFrame by selecting relevant fields from
        the nested structure. It also creates separate columns for
        longitude, latitude, and depth extracted from the coordinates.
        Args:
            source_blob_filepath (str): The file path or URL of the JSON file
                                         to be read and flattened.
            spark_app_name (str): The name of the Spark application.
        Returns:
            pyspark.sql.dataframe.DataFrame: A flattened DataFrame containing
                                              the extracted fields and additional
                                              columns for longitude, latitude,
                                              and depth.
        Raises:
            pyspark.sql.utils.AnalysisException: If there is an issue with
                                                 reading or processing the JSON data.
    """
    df = CreateDataFrame(bucket_name)
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
            .withColumn("depth", flattened_df["coordinates"].getItem(2))  # Accessing the coordinates properly using selectExpr or a similar method
    flattened_df = flattened_df.drop("coordinates")  # Drop the original coordinates and geometry columns which are not needed
    return flattened_df


def column_transformation(bucket_name):
    """
        Applies various transformations to a flattened Spark DataFrame containing earthquake data.

        This function takes a DataFrame that has been flattened from a nested JSON structure and
        performs several transformations, including:
        - Converting epoch time (in milliseconds) to timestamp format for the 'time' and 'updated' columns.
        - Extracting the area from the 'place' column using a regular expression.
        - Adding a current timestamp to indicate when the data was processed.

        Args:
            bucket_name (str): The name of the Google Cloud Storage bucket containing the source JSON file.

        Returns:
            pyspark.sql.dataframe.DataFrame: A transformed DataFrame with the following modifications:
                - 'time' column converted from epoch milliseconds to a timestamp.
                - 'updated' column converted from epoch milliseconds to a timestamp.
                - 'area' column extracted from the 'place' column using regex.
                - 'insert_dt' column containing the current timestamp.

        Raises:
            pyspark.sql.utils.AnalysisException: If there is an issue with transforming the DataFrame
                                                 or if the DataFrame does not match the expected structure.
        """
    flattenDF = FlattenDF(bucket_name)
    flattenDF = flattenDF.withColumn("time", from_unixtime(col("time") / 1000).cast("timestamp")) \
                                         .withColumn("updated", from_unixtime(col("updated")/ 1000).cast("timestamp"))  \
                                         .withColumn("area", regexp_extract(col("place"), r"of (.+)$", 1)) \
                                         .withColumn("insert_dt", current_timestamp())
    return flattenDF

def load_data_gcs_silver_path(bucket_name):
    """
    Generates a unique GCS path and saves the DataFrame in Parquet format.

    Parameters:
    bucket_name (str): The name of the GCS bucket.

    Returns:
    str: The GCS path where the Parquet file is saved.
"""
    date_str = datetime.now().strftime('%Y%m%d')
    gcs_path = f"gs://{bucket_name}/silver/pyspark/landing/{date_str}/transformed_data"

    # Perform column transformation on DataFrame
    df = column_transformation(bucket_name)

    # Write the DataFrame to Parquet format in GCS
    df.write.parquet(gcs_path, mode='overwrite')

    return gcs_path


def load_parquet_to_bigquery_from_gcs(bucket_name, dataset_id, table_id, project_id):
    """
    Loads a Parquet file from GCS into a BigQuery table, creating the dataset and table if they don't exist.

    Parameters:
    bucket_name (str): Name of the GCS bucket.
    dataset_id (str): BigQuery dataset ID.
    table_id (str): BigQuery table ID.
    project_id (str): GCP project ID.

    Returns:
    None
    """
    # Define the path to the Parquet file in GCS
    date_str = datetime.now().strftime('%Y%m%d')
    gcs_path = f"gs://{bucket_name}/silver/pyspark/landing/{date_str}/transformed_data/*.parquet"

    # Define the schema for the BigQuery table
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

    try:
            # Initialize the BigQuery client
        client = bigquery.Client(project=project_id)

            # Define the full dataset reference
        dataset_ref = f"{project_id}.{dataset_id}"

            # Check if the dataset exists
        try:
            client.get_dataset(dataset_ref)
            print(f"Dataset '{dataset_id}' already exists.")
        except Exception:
                # Create the dataset if it doesn't exist
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Set the location as needed
            client.create_dataset(dataset)
            print(f"Created dataset '{dataset_id}'.")

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,  # Specify the file format
        schema=schema,  # Use the provided schema
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append to the table if it exists
        )

        # Start the load job
        load_job = client.load_table_from_uri(
        gcs_path,
            f"{dataset_ref}.{table_id}",
            job_config=job_config
        )

        # Wait for the load job to complete
        load_job.result()
        print(f"Loaded {load_job.output_rows} rows into {dataset_ref}.{table_id}.")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

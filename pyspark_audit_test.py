#Script name:hist_pycharm.py
#Script Description:Historical data extracted and transformed and loaded into bigquery
#Update date:11/11/2024
from google.cloud import bigquery
#######################################################


from pyspark.sql import SparkSession

from audit_new import CloudStorageUploader, SparkDataProcessor, BigQueryLoader
import config

if __name__ == '__main__':

    # Initialize the BigQuery client
    client = bigquery.Client()

    # Define table ID (replace with your actual project and dataset)
    table_id = f"{config.project_id}.{config.dataset_id}.pyspark_audit_table"

    # Define the schema based on the given StructType
    schema = [
        bigquery.SchemaField("job_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pipeline_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("function_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("start_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("end_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("error_msg", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("process_record", "INTEGER", mode="NULLABLE")
    ]

    # Create table object with the new schema
    table = bigquery.Table(table_id, schema=schema)

    # Create the table in BigQuery
    table = client.create_table(table, exists_ok=True)
    print(f"Created table {table_id}")

    # Define Spark session
    spark =SparkSession.builder.master('local[*]').appName('Historical and daily load').getOrCreate()


    # Step 1: Initialize Cloud Storage Uploader and upload data to GCS
    cloud_storage_uploader = CloudStorageUploader(bucket_name=config.bucket_name)



    # Upload data from the URL to GCS (Bronze Layer)
    upload_success = cloud_storage_uploader.upload_data_to_bucket_bronze_layer(config.hist_url1)
    if upload_success:
        print("Data uploaded to GCS bronze layer successfully.")


    # Step 2: Initialize Spark Data Processor and transform data
    spark_processor = SparkDataProcessor()


    # Transform data and save it to GCS as Parquet (Silver Layer)
    transformed_data_path = spark_processor.save_to_gcs(bucket_name=config.bucket_name, layer="silver")
    print(f"Transformed data saved to: {transformed_data_path}")


    # Step 3: Initialize BigQuery Loader and load data from GCS to BigQuery
    bq_loader = BigQueryLoader(project_id=config.project_id)


    # Load the transformed data into BigQuery
    bq_loader.load_parquet_to_bigquery(
        bucket_name=config.bucket_name,
        dataset_id=config.dataset_id,
        table_id=config.table_id2
    )
    print(f"Data loaded to BigQuery table {config.dataset_id}.{config.table_id2} successfully.")

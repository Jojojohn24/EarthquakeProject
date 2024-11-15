#Script name:hist_pycharm.py
#Script Description:Historical data extracted and transformed and loaded into bigquery
#Update date:11/11/2024

#######################################################


from pyspark.sql import SparkSession

from UtilsNew import CloudStorageUploader,DataFetcher,SparkDataProcessor,BigQueryLoader
import config

if __name__ == '__main__':

    # Define Spark session
    spark =SparkSession.builder.master('local[*]').appName('Historical and daily load').getOrCreate()


    # Step 1: Initialize Cloud Storage Uploader and upload data to GCS
    cloud_storage_uploader = CloudStorageUploader(bucket_name=config.bucket_name)



    # Upload data from the URL to GCS (Bronze Layer)
    upload_success = cloud_storage_uploader.upload_data_to_bucket_bronze_layer(config.hist_url)
    if upload_success:
        print("Data uploaded to GCS bronze layer successfully.")


    # Step 2: Initialize Spark Data Processor and transform data
    spark_processor = SparkDataProcessor(app_name="DataPipelineSparkApp")


    # Transform data and save it to GCS as Parquet (Silver Layer)
    transformed_data_path = spark_processor.save_to_gcs(bucket_name=config.bucket_name, layer="silver")
    print(f"Transformed data saved to: {transformed_data_path}")


    # Step 3: Initialize BigQuery Loader and load data from GCS to BigQuery
    bq_loader = BigQueryLoader(project_id=config.project_id)


    # Load the transformed data into BigQuery
    bq_loader.load_parquet_to_bigquery(
        bucket_name=config.bucket_name,
        dataset_id=config.dataset_id,
        table_id=config.table_id1
    )
    print(f"Data loaded to BigQuery table {config.dataset_id}.{config.table_id1} successfully.")

# Script name:daily_data_main_file
# Script Description: daily data  extracted,transformed and appended to bigquery
# Update date:26/10/2024

#######################################################


from pyspark.sql import SparkSession

from Utils import CreateDataFrame,\
    download_as_text_bronze_gcs, FlattenDF, column_transformation, load_data_gcs_silver_path, append_to_bigquery

if __name__ == '__main__':
    # Define Spark session
    spark = SparkSession.builder.master('local[*]').appName('Historical and daily load').getOrCreate()

    # create bucket

    # bucket_name = 'earthquake_analysis12'
    # project_id = 'bwt-project-431809'
    # bucket = initialize_gcs_and_create_bucket(bucket_name, project_id)

    # Pulling data from API
    url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson'

    # earthquake_data = fetch_earthquake_data(url)

    # print(earthquake_data)

    # Uploading extracted raw data to GCS
    # layer = 'bronze'
    # bucket_name = 'earthquake_analysis12'
    # upload_earthquake_data_to_gcs(url,bucket_name)

    # Downloading the raw data from GCS bucket as text
    bucket_name = 'earthquake_analysis12'
    download_as_text_bronze_gcs(bucket_name)

    # Creating Dataframe from the raw data

    CreateDataFrame(bucket_name)

    # df.show()

    # ****************************************************************
    # Flattening the data

    FlattenDF(bucket_name)
    # print(flattened_df.show(truncate=False))

    # Column transformations and saving into parquet
    df = column_transformation(bucket_name)
    print(df.show(truncate=False))

    # loading data into silver layer

    load_data_gcs_silver_path(bucket_name)

    # appending table on bigqueryy

    bucket_name = 'earthquake_analysis12'
    dataset_id = 'earthquake_db'
    table_id = 'earthquake_data'
    project_id = 'bwt-project-431809'
    temp_gcs_bucket = 'gcs_temp_bigquery'
    append_to_bigquery(df, project_id, dataset_id, table_id,temp_gcs_bucket)

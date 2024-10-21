#Script name:
#Script Description:
#Update date:
from datetime import datetime

#######################################################


from pyspark.sql import SparkSession
import json
import argparse
import requests
import os
from google.cloud import storage
import pyarrow.parquet
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DecimalType, ArrayType, \
    FloatType

if __name__ == '__main__':

    # Define Spark session
    spark =SparkSession.builder.master('local[*]').appName('Historical and daily load').getOrCreate()


    #Pulling data from API

    url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'


    # using python request lib fetching data from given url
    response = requests.get(url)


    data = response.json()

    # print(type(data))

    # print(data)


    # # initialization of google application
    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\cyril\JojoProjects\EarthquakeProject\bwt-project-431809-db37d49b0270.json'

    client = storage.Client(project='bwt-project-431809')

    # print(client.project)


    # create bucket

    # bucket_name = 'earthquake_analysis12'
    #
    # bucket = client.bucket(bucket_name)
    # bucket.storage_class = 'STANDARD'
    # new_bucket =client.create_bucket(bucket,location='us-central1')
    #
    # print('bucket created successfully')

    # client = storage.Client()
    # bucket = client.bucket('earthquake_analysis12')
    #
    # url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
    # response = requests.get(url)
    # if response.status_code == 200:
    #     data = response.json()
    #     date_str = datetime.now().strftime('%Y%m%d')
    #     filename = f"pyspark/landing/{date_str}/raw_data.json"
    #     blob = bucket.blob(f'bronze/{filename}')
    #     # Upload the JSON data directly from memory to GCS
    #     blob.upload_from_string(data=json.dumps(data), content_type='application/json')
    #
    #     print(f"Upload of {filename} complete.")
    # else:
    #     print("Upload failed.")





    # Define the schema
    for feature in data['features']:
        feature['properties']['mag'] = float(feature['properties']['mag']) if feature['properties'][
                                                                                  'mag'] is not None else None
        feature['properties']['gap'] = float(feature['properties']['gap']) if feature['properties'][
                                                                                  'gap'] is not None else None
        feature['properties']['dmin'] = float(feature['properties']['dmin']) if feature['properties'][
                                                                                    'dmin'] is not None else None
        feature['properties']['rms'] = float(feature['properties']['rms']) if feature['properties'][
                                                                                  'rms'] is not None else None  # Convert rms to float
        feature['geometry']['coordinates'] = [float(coord) for coord in feature['geometry']['coordinates']] if \
        feature['geometry']['coordinates'] is not None else []

    # Define the schema
    schema = StructType([
        StructField("type", StringType(), True),
        StructField("properties", StructType([
            StructField("mag", FloatType(), True),
            StructField("place", StringType(), True),
            StructField("time", LongType(), True),
            StructField("updated", LongType(), True),
            StructField("tz", StringType(), True),
            StructField("url", StringType(), True),
            StructField("detail", StringType(), True),
            StructField("felt", StringType(), True),
            StructField("cdi", StringType(), True),
            StructField("mmi", StringType(), True),
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
            StructField("gap", FloatType(), True),  # Change to FloatType
            StructField("magType", StringType(), True),
            StructField("title", StringType(), True)
        ]), True),
        StructField("geometry", StructType([
            StructField("type", StringType(), True),
            StructField("coordinates", ArrayType(FloatType()), True)
        ]), True),
        StructField("id", StringType(), True)
    ])
    df = spark.createDataFrame(data['features'], schema=schema)
    # print(df.show(truncate=False))

    flattened_df = df.select(
        "properties.mag",
        "properties.place",
        "properties.time",
        "properties.updated",
        "properties.tz",
        "properties.url",
        "properties.detail",
        "properties.felt",
        "properties.cdi",
        "properties.mmi",
        "properties.alert",
        "properties.status",
        "properties.tsunami",
        "properties.sig",
        "properties.net",
        "properties.code",
        "properties.ids",
        "properties.sources",
        "properties.types",
        "properties.nst",
        "properties.dmin",
        "properties.rms",
        "properties.gap",
        "properties.magType",
        "properties.title",
        "geometry.coordinates"
    )

    # Accessing the coordinates properly using selectExpr or a similar method
    flattened_df = flattened_df.withColumn("longitude", flattened_df["coordinates"].getItem(0)) \
        .withColumn("latitude", flattened_df["coordinates"].getItem(1)) \
        .withColumn("depth", flattened_df["coordinates"].getItem(2))

    # Drop the original coordinates and geometry columns if not needed
    flattened_df = flattened_df.drop("coordinates")

    # Show the flattened DataFrame
    flattened_df.show(truncate=False)







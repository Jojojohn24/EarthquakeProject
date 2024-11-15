import os
import logging
import json
from datetime import datetime
import requests
import apache_beam as beam
import pyarrow as pa

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class FetchAndUpload(beam.DoFn):
    def __init__(self, api_url, bucket_name, destination_file_name):
        self.api_url = api_url
        self.bucket_name = bucket_name
        self.destination_file_name = destination_file_name

    def fetch_data_from_api(self, api_url):
        try:
            logging.info(f">> Fetching raw data from an API: {api_url}")
            response = requests.get(api_url)
            response.raise_for_status()  # Raise an error for bad status codes
            logging.info(f">> SUCCESSFUL: Raw data fetched successfully.")
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            logging.error(f'HTTP error occurred: {http_err}')
            raise
        except Exception as err:
            logging.error(f'An error occurred: {err}')
            raise

    def upload_data_to_gcs(self, bucket_name: str, destination_file_name: str, data: dict) -> None:
        try:
            from google.cloud import storage
            storage_client = storage.Client()
            logging.info(
                f">> Uploading raw data (fetched from URL) to GCS bucket: {bucket_name}, file: {destination_file_name}")
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_file_name)
            blob.upload_from_string(json.dumps(data), content_type='application/json')
            logging.info(
                f'>> SUCCESSFUL: Raw data (fetched from URL) uploaded successfully in json format to : {bucket_name}/{destination_file_name}')
        except Exception as err:
            logging.error(f'>> Failed to upload Raw data (fetched from URL) to GCS: {err}')
            raise

    def process(self, element):
        data = self.fetch_data_from_api(self.api_url)
        self.upload_data_to_gcs(self.bucket_name, self.destination_file_name, data)
        output_filename = f"gs://{self.bucket_name}/{self.destination_file_name}/raw_data_fetched.json"
        # yield output_filename


class FlattenPlusTransformations(beam.DoFn):
    def process(self, element):
        def safe_cast(value, target_type, default):
            """Safely cast a value to a specified type with a default fallback."""
            try:
                if value is not None:
                    return target_type(value)
            except (ValueError, TypeError):
                return default
            return default

        try:
            input_data = json.loads(element)
            features = input_data['features']

            for feature in features:
                properties = feature.get('properties', {})
                geometry = feature.get('geometry', {}).get('coordinates', [None, None, None])
                place = properties.get('place', "")
                area = place.split(" of ")[1] if " of " in place else None
                event_time = properties.get('time')
                last_update = properties.get('updated')
                insert_dt = datetime.utcnow().isoformat()

                # Flattened result dictionary for Silver Layer
                result_for_silver_layer = {
                    'magnitude': safe_cast(properties.get('mag'), float, 0.0),
                    'place':safe_cast(properties.get('place'),str, ""),
                    'event_time': datetime.utcfromtimestamp(
                        safe_cast(event_time, int, 0) / 1000).isoformat() if event_time else None,
                    'last_update': datetime.utcfromtimestamp(
                        safe_cast(last_update, int, 0) / 1000).isoformat() if last_update else None,
                    'timezone_offset': safe_cast(properties.get('tz'), int, 0),
                    'info_url': properties.get('url') or "",
                    'description': properties.get('detail') or "",
                    'felt_reports': safe_cast(properties.get('felt'), int, 0),
                    'cdi_value': safe_cast(properties.get('cdi'), float, 0.0),
                    'mmi_value': safe_cast(properties.get('mmi'), float, 0.0),
                    'alert_status': properties.get('alert') or "",
                    'event_status': properties.get('status') or "",
                    'tsunami_warning': safe_cast(properties.get('tsunami'), int, 0),
                    'significance': safe_cast(properties.get('sig'), int, 0),
                    'network_code': properties.get('net') or "",
                    'event_code': properties.get('code') or "",
                    'event_ids': properties.get('ids') or "",
                    'data_sources': properties.get('sources') or "",
                    'event_types': properties.get('types') or "",
                    'station_count': safe_cast(properties.get('nst'), int, 0),
                    'min_distance': safe_cast(properties.get('dmin'), float, 0.0),
                    'rms_value': safe_cast(properties.get('rms'), float, 0.0),
                    'gap_angle': safe_cast(properties.get('gap'), float, 0.0),
                    'magnitude_type': properties.get('magType') or "",
                    'event_type': properties.get('type') or "",
                    'longitude': safe_cast(geometry[0], float, 0.0),
                    'latitude': safe_cast(geometry[1], float, 0.0),
                    'depth': safe_cast(geometry[2], float, 0.0),
                    'area': area or ""
                }
                yield beam.pvalue.TaggedOutput('silver', result_for_silver_layer)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON: {e}")
        except Exception as e:
            logging.error(f"Error processing element: {e}")


class ProcessForBigQuery(beam.DoFn):
    def process(self, element):
        # Get the current datetime in ISO format
        current_datetime = datetime.utcnow().isoformat()

        # Prepare the output record with insert_dt
        output_record = {
            'magnitude': element['magnitude'],
            'place':element['place'],
            'event_time': element['event_time'],
            'last_update': element['last_update'],
            'timezone_offset': element['timezone_offset'],
            'info_url': element['info_url'],
            'description': element['description'],
            'felt_reports': element['felt_reports'],
            'cdi_value': element['cdi_value'],
            'mmi_value': element['mmi_value'],
            'alert_status': element['alert_status'],
            'event_status': element['event_status'],
            'tsunami_warning': element['tsunami_warning'],
            'significance': element['significance'],
            'network_code': element['network_code'],
            'event_code': element['event_code'],
            'event_ids': element['event_ids'],
            'data_sources': element['data_sources'],
            'event_types': element['event_types'],
            'station_count': element['station_count'],
            'min_distance': element['min_distance'],
            'rms_value': element['rms_value'],
            'gap_angle': element['gap_angle'],
            'magnitude_type': element['magnitude_type'],
            'event_type': element['event_type'],
            'longitude': element['longitude'],
            'latitude': element['latitude'],
            'depth': element['depth'],
            'area': element['area'],
            'insert_dt': current_datetime
        }
        yield output_record


schema = {
    'fields': [
        {'name': 'magnitude', 'type': 'FLOAT'},
        {'name': 'place', 'type': 'STRING'},
        {'name': 'event_time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'last_update', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'timezone_offset', 'type': 'INTEGER'},
        {'name': 'info_url', 'type': 'STRING'},
        {'name': 'description', 'type': 'STRING'},
        {'name': 'felt_reports', 'type': 'INTEGER'},
        {'name': 'cdi_value', 'type': 'FLOAT'},
        {'name': 'mmi_value', 'type': 'FLOAT'},
        {'name': 'alert_status', 'type': 'STRING'},
        {'name': 'event_status', 'type': 'STRING'},
        {'name': 'tsunami_warning', 'type': 'INTEGER'},
        {'name': 'significance', 'type': 'INTEGER'},
        {'name': 'network_code', 'type': 'STRING'},
        {'name': 'event_code', 'type': 'STRING'},
        {'name': 'event_ids', 'type': 'STRING'},
        {'name': 'data_sources', 'type': 'STRING'},
        {'name': 'event_types', 'type': 'STRING'},
        {'name': 'station_count', 'type': 'INTEGER'},
        {'name': 'min_distance', 'type': 'FLOAT'},
        {'name': 'rms_value', 'type': 'FLOAT'},
        {'name': 'gap_angle', 'type': 'FLOAT'},
        {'name': 'magnitude_type', 'type': 'STRING'},
        {'name': 'event_type', 'type': 'STRING'},
        {'name': 'longitude', 'type': 'FLOAT'},
        {'name': 'latitude', 'type': 'FLOAT'},
        {'name': 'depth', 'type': 'FLOAT'},
        {'name': 'area', 'type': 'STRING'},
        {'name': 'insert_dt', 'type': 'TIMESTAMP'}
    ]
}

parquet_schema = pa.schema([
    ('magnitude', pa.float32()),
    ('place',pa.string()),
    ('event_time', pa.string()),
    ('last_update', pa.string()),
    ('timezone_offset', pa.int32()),
    ('info_url', pa.string()),
    ('description', pa.string()),
    ('felt_reports', pa.int32()),
    ('cdi_value', pa.float32()),
    ('mmi_value', pa.float32()),
    ('alert_status', pa.string()),
    ('event_status', pa.string()),
    ('tsunami_warning', pa.int32()),
    ('significance', pa.int32()),
    ('network_code', pa.string()),
    ('event_code', pa.string()),
    ('event_ids', pa.string()),
    ('data_sources', pa.string()),
    ('event_types', pa.string()),
    ('station_count', pa.int32()),
    ('min_distance', pa.float32()),
    ('rms_value', pa.float32()),
    ('gap_angle', pa.float32()),
    ('magnitude_type', pa.string()),
    ('event_type', pa.string()),
    ('longitude', pa.float32()),
    ('latitude', pa.float32()),
    ('depth', pa.float32()),
    ('area', pa.string())
])

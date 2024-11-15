from datetime import datetime


daily_url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson' # Replace with the actual URL

hist_url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'
hist_url1 = 'https://earthquake.usgs.gov/earthquakes/feeed/v1.0/summary/all_month.geojson'

bucket_name = 'earthquake97'  # Replace with your GCS bucket name
project_id = "seismic-operand-441006-c9"  # Replace with your GCP project ID
dataset_id = 'earthquake_df'  # Replace with your BigQuery dataset ID
table_id = 'earthquake_data_pycharm'  # Replace with your BigQuery table ID
table_id1 = 'earthquake_data_pycharm1'
table_id2 = 'earthquake_data_pycharm_ttest'
DATAFLOW_STAGING_LOCATION = f"gs://{bucket_name}/dataflow/stage_location1"
TEMP_LOCATION = f"gs://{bucket_name}/dataflow/temp_location1"
SERVICE_ACCOUNT_KEY = r"C:\Users\cyril\JojoProjects\EarthquakeProject\seismic-operand-441006-c9-eb218210584d.json"
current_date = datetime.now().strftime('%Y%m%d')
DATAFLOW_LANDING_LOCATION = f"bronze/dataflow/{current_date}/raw_data.json"
DATAFLOW_LANDING_LOCATION1 = f"bronze/dataflow/daily/{current_date}/raw_data.json"

DATAFLOW_SILVER_LAYER_PATH = f"Silver/dataflow/{current_date}/transformed_data"
DATAFLOW_SILVER_LAYER_PATH1 = f"Silver/dataflow/daily/{current_date}/transformed_data"
DATAFLOW_GOLD_LAYER_PATH = f"Gold/dataflow/{current_date}/final_data"
TABLE_ID_df = "earthquake_data_df"
job_id = 2


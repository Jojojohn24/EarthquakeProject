from google.cloud.bigquery import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

def createDFforAuditTbl(spark_1, job_id, pipeline_name, function_name, start_time, end_time, status, error_msg, process_record=0):
    # Create audit entries using Row
    audit_entry = [Row(
        job_id=job_id,
        pipeline_name=pipeline_name,
        function_name=function_name,
        start_time=start_time,
        end_time=end_time,
        status=status,
        error_msg=error_msg,
        process_record=process_record
    )]

    # Define schema for the audit DataFrame
    schema = StructType([
        StructField('job_id', StringType(), True),
        StructField('pipeline_name', StringType(), True),
        StructField('function_name', StringType(), True),
        StructField('start_time', TimestampType(), True),
        StructField('end_time', TimestampType(), True),
        StructField('status', StringType(), True),
        StructField('error_msg', StringType(), True),
        StructField('process_record', IntegerType(), True)
    ])

    # Create DataFrame with the provided schema
    audit_df = spark_1.createDataFrame(audit_entry, schema)

    return audit_df

def log_audit(self, spark, job_id, pipeline_name, function_name, start_time, end_time, status, process_record, audit_output_db, error_msg):
    # Generate audit DataFrame
    audit_df = createDFforAuditTbl(spark, job_id, pipeline_name, function_name, start_time, end_time, status, error_msg, process_record)

    # Define the BigQuery schema for the audit table (remove if it's defined elsewhere)
    audit_table_schema = [
        {"name": "job_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "pipeline_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "function_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "start_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "end_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "error_msg", "type": "STRING", "mode": "NULLABLE"},
        {"name": "process_record", "type": "INTEGER", "mode": "NULLABLE"}
    ]

    # Write the audit DataFrame to BigQuery
    self.writeDataintoBigquery(audit_output_db, audit_df, audit_table_schema)

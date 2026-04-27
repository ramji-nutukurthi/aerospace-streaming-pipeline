"""
Pipeline: Dimension Data Processing
Layer: Bronze
Architecture: PySpark Batch Processing
Purpose: Ingest daily ERP JSON snapshots from S3 into Bronze Dimension Tables.
Philosophy: OVERWRITE mode ensures the Bronze layer reflects the exact current state of the ERP.
"""

from pyspark.sql.functions import current_timestamp

# --- Global Configuration ---
BUCKET_NAME = "aerospace-telemetry-raw"

DIMENSIONS = [
    'dim_engines',
    'dim_locations',
    'dim_aircraft',
    'dim_parts_bom',
    'dim_thresholds'
]

def ingest_dimensions_to_bronze(spark):
    for dim_name in DIMENSIONS:
        source_path = f"s3://{BUCKET_NAME}/erp_export/{dim_name}/"
        target_table = f"aerospace.bronze.brz_{dim_name}"

        # 1. READ
        try:
            df_raw = spark.read.format("json").load(source_path)
        except Exception as e:
            print(f"Warning: Skipping {dim_name}. Could not read from S3. Error: {str(e)}")
            continue # Skip to the next dimension in the loop

        # 2. TRANSFORM
        df_bronze = df_raw.withColumn("ingestion_timestamp", current_timestamp())

        # 3. WRITE
        try:
            df_bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
            
        except Exception as e:
            print(f"Error writing {target_table} to Bronze. Error: {str(e)}")

if __name__ == "__main__":
    # In Databricks, 'spark' is automatically available in the global scope.
    ingest_dimensions_to_bronze(spark)
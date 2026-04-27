"""
Pipeline: Fact Data Processing
Layer: Bronze
Architecture: PySpark structured streaming (Databricks Auto Loader)
Purpose: Ingest raw telemetry JSON from S3 into the Bronze fact table.
Features: Relies on native Auto Loader checkpoints; deferes archiving to AWS Lifecycle rules.
"""

from pyspark.sql.functions import current_timestamp, col

# --- Global Configuration ---
BUCKET_NAME = "aerospace-telemetry-raw"

def ingest_telemetry_to_bronze(spark):
    source_path = f"s3://{BUCKET_NAME}/bronze/"
    checkpoint_path = f"s3://{BUCKET_NAME}/checkpoints/brz_fact_telemetry/"
    target_table = "aerospace.bronze.brz_fact_telemetry"

    # 1. Read raw stream
    df_raw_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", checkpoint_path)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
    )

    # 2. Add Unity Catalog Audit Metadata
    df_enriched_stream = (
        df_raw_stream
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file_path", col("_metadata.file_path"))
    )

    # 3. Write to Delta (Append-only for immute sensor facts)
    query = (
        df_enriched_stream.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True) #availabaleNow=True process everything currently in s3 and then gracefully stops.
        .outputMode("append")
        .option("mergeSchema", "true")
        .table(target_table)
    )

    query.awaitTermination()

if __name__ == "__main__":
    #In Databricks, 'spark' is automatically available in the global scope.
    ingest_telemetry_to_bronze(spark)
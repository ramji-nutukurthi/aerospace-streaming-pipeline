"""
Pipeline: Critical Maintenance Alerts
Layer: Gold
Architecture: PySpark structured streaming
Purpose: Cross-reference live engine telemetry against physical limits to generate real-time alerts.
Features: Unpivots sensor columns into rows for dynamic threshold joins.
"""

from pyspark.sql.functions import col, current_timestamp

# --- Global Configuration ---
BUCKET_NAME = "aerospace-telemetry-raw"

def run_maintenance_alerts_stream(spark):

    # Read Static dimensions and live stream
    
    df_thresholds = spark.read.table("aerospace.silver.slv_dim_thresholds")
    df_telemetry_stream = spark.readStream.table("aerospace.silver.slv_fact_telemetry")
    checkpoint_path = f"s3://{BUCKET_NAME}/checkpoints/gld_critical_alerts/"
    target_table = "aerospace.gold.gld_critical_alerts"

    # Unpivot the sensors (Convert columns into rows to join against the thresholds table)
    unpivot_expr = """
        stack(5,
        'temp_hpc_out', temp_hpc_out,
        'temp_lpt_out', temp_lpt_out,
        'press_hpc_out', press_hpc_out,
        'physical_core_speed', physical_core_speed,
        'engine_pressure_ratio', engine_pressure_ratio
        ) as (sensor_name, sensor_value)
    """

    df_stream_unpivoted = df_telemetry_stream.selectExpr(
        "engine_id",
        "cycle",
        "telemetry_timestamp as event_time",
        unpivot_expr
    )

    # Stream-Static Join to detect anomalies
    df_alerts = (
        df_stream_unpivoted
        .join(df_thresholds, on='sensor_name', how='inner')
        # Trigger an alert Only if the live value exceeds the documented critical limit
        .filter(col("sensor_value") > col("critical_limit"))
        .withColumn("alert_generated_at", current_timestamp())
        .select(
            "engine_id", "cycle", "event_time",
            "sensor_name", "sensor_value", "critical_limit",
            "alert_description", "alert_generated_at"
        )
    )

    # Write Alerts to Gold (Append Only)
    query = (
        df_alerts.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .outputMode("append")
        .option("mergeSchema", "true")
        .table(target_table)
    )

    query.awaitTermination()

if __name__ == "__main__":
    run_maintenance_alerts_stream(spark)
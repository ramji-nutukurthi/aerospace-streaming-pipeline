"""
Pipeline: Silver Telemetry Stream
Layer: Silver
Architecture: PySpark Structured Streaming with Dead Letter Queue (DLQ)
Purpose: Transform raw sensor data, inject timestamps, and quarantine impossible readings.
"""

from pyspark.sql.functions import col, current_timestamp, when

# --- Global Configuration ---
BUCKET_NAME = "aerospace-telemetry-raw"

def write_to_silver_and_quarantine(df_microbatch, batch_id):
    """
    Evaluates the CLEANED data for physical impossibilities and routes it.
    """
    # 1. Define Data Quality Rules
    df_evaluated = df_microbatch.withColumn(
        "is_quarantined",
        (col("engine_id").isNull()) | 
        (col("temp_hpc_out") < -50) |       
        (col("physical_core_speed") < 0)    
    ).withColumn(
        "quarantine_reason",
        when(col("engine_id").isNull(), "CRITICAL: Missing Engine ID")
        .when(col("temp_hpc_out") < -50, "ERROR: Impossible negative temperature")
        .when(col("physical_core_speed") < 0, "ERROR: Negative RPM detected")
        .otherwise("Valid")
    )

    # 2. Split the Data
    df_valid = (df_evaluated
                .filter(col("is_quarantined") == False)
                .drop("is_quarantined", "quarantine_reason"))
    
    df_quarantined = df_evaluated.filter(col("is_quarantined") == True)

    # 3. Write to Target Tables
    if not df_valid.isEmpty():
        (df_valid.write
         .format("delta")
         .mode("append")
         .saveAsTable("aerospace.silver.slv_fact_telemetry"))
        
    if not df_quarantined.isEmpty():
        (df_quarantined.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true") 
         .saveAsTable("aerospace.silver.slv_quarantine_telemetry"))

def run_silver_stream(spark):
    
    # 1. Read the raw Bronze stream
    df_bronze_stream = spark.readStream.table("aerospace.bronze.brz_fact_telemetry")
    checkpoint_path = f"s3://{BUCKET_NAME}/checkpoints/slv_fact_telemetry/"

    # 2. TRANSFORM THE CONTINUOUS STREAM (Before routing)
    df_transformed_stream = (
        df_bronze_stream
        .withColumns({
            # Cast Identifiers
            "engine_id": col("engine_id").cast("integer"),
            "cycle": col("cycle").cast("integer"),
            # Translate settings
            "altitude_ft": col("setting1").cast("double"),
            "mach_number": col("setting2").cast("double"),
            "throttle_angle": col("setting3").cast("double"),
            # Translate Sensor codes
            "temp_fan_inlet": col("s1").cast("double"),
            "temp_lpc_out": col("s2").cast("double"),   
            "temp_hpc_out": col("s3").cast("double"),   
            "temp_lpt_out": col("s4").cast("double"),   
            "press_fan_inlet": col("s5").cast("double"),
            "press_bypass": col("s6").cast("double"),
            "press_hpc_out": col("s7").cast("double"),
            "physical_fan_speed": col("s8").cast("double"),
            "physical_core_speed": col("s9").cast("double"),
            "engine_pressure_ratio": col("s10").cast("double"),
            # Inject Timestamps
            "telemetry_timestamp": current_timestamp(),
            "silver_processing_timestamp": current_timestamp()
        })
        # Data Governance: Drop raw cryptic columns
        .drop("setting1", "setting2", "setting3", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "_rescued_data")
    )

    # 3. Pass the TRANSFORMED stream into the foreachBatch router
    query = (
        df_transformed_stream.writeStream
        .foreachBatch(write_to_silver_and_quarantine)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )
    
    query.awaitTermination()

if __name__ == "__main__":
    run_silver_stream(spark)
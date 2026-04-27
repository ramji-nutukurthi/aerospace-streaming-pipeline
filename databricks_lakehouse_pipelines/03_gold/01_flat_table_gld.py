"""
Pipeline: Analytical Data
Layer: Gold
Architecture: PySpark Batch Processing
Purpose: Build a denormalized One Big Table (OBT) and an aggregated Daily Fleet Summary.
Features: Z-Order Optimization for fast BI queries, Native Data extraction.
"""

from pyspark.sql.functions import col, avg, max as spark_max, count, to_date, round, current_timestamp

def build_gold_obt(spark):
    # Read the clean silver tables
    df_facts = spark.read.table("aerospace.silver.slv_fact_telemetry")
    df_engines = spark.read.table("aerospace.silver.slv_dim_engines").drop("silver_processing_timestamp")
    df_aircraft = spark.read.table("aerospace.silver.slv_dim_aircraft").drop("silver_processing_timestamp")
    df_locations = spark.read.table("aerospace.silver.slv_dim_locations").drop("silver_processing_timestamp")
    df_facts_dated = df_facts.withColumn("telemetry_date", to_date(col("telemetry_timestamp")))

    # Build the Denormalized OBT
    df_obt = (
        df_facts_dated
        .join(df_engines, "engine_id", "left")
        .join(df_aircraft, "tail_number", "left")
        .join(df_locations, df_engines.home_location_id == df_locations.location_id, "left")
        .drop("source_file_path", "ingestion_timestamp", "silver_processing_timestamp", "location_id") # Drop raw audit columns
        .withColumn("gold_obt_updated_at", current_timestamp())
    )

    # Save and Optimize
    target_table = "aerospace.gold.gld_obt_telemetry"
    df_obt.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

    spark.sql(f"OPTIMIZE {target_table} ZORDER BY (telemetry_date, engine_id)")

def build_aggregated_data(spark):
    df_obt = spark.read.table("aerospace.gold.gld_obt_telemetry")

    # Aggregated by Date, Airline, and Region
    df_data = (
        df_obt
        .filter(col("airline_owner").isNotNull() & col("region").isNotNull())
        .groupBy("telemetry_date", "airline_owner", "region")
        .agg(
            count("engine_id").alias("total_sensor_pings"),
            round(avg("temp_hpc_out"), 2).alias("avg_high_pressure_compressor_temp"),
            round(spark_max("physical_core_speed"), 2).alias("max_core_speed_rpm")
        )
    )

    df_data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("aerospace.gold.gld_fleet_daily_summary")

def create_security_views(spark):
    spark.sql("""
              CREATE OR REPLACE VIEW aerospace.gold.vw_emirates_dashboard AS
              SELECT * FROM aerospace.gold.gld_fleet_daily_summary WHERE airline_owner = 'Emirates'
    """)

if __name__ == "__main__":
    build_gold_obt(spark)
    build_aggregated_data(spark)
    create_security_views(spark)
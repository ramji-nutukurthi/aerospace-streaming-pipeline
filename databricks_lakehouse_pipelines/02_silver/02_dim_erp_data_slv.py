"""
Pipeline: Dimension Data Processing
Layer: Silver
Architecture: PySpark Batch Processing
"""
from pyspark.sql.functions import col, initcap, trim, lower, to_date, coalesce, current_timestamp, expr

def process_silver_dimensions(spark):
    tables_to_process = ["dim_locations", "dim_aircraft", "dim_engines", "dim_parts_bom", "dim_thresholds"]

    for dim in tables_to_process:
        source_table = f"aerospace.bronze.brz_{dim}"
        target_table = f"aerospace.silver.slv_{dim}"

        # READ
        try:
            df_raw = spark.read.table(source_table)
        except Exception as e:
            print(f"Skipping {dim}. Bronze table missing. Error: {str(e)}")
            continue

        # TRANSFORM
        if dim == "dim_locations":
            df_clean = (df_raw.fillna({"climate": "unknown"})
                        .withColumn("city", initcap(trim(col("city"))))
                        .withColumn("airport", initcap(trim(col("airport"))))
                        .withColumn("region", initcap(trim(col("region"))))
                        .withColumn("climate", lower(trim(col("climate"))))
                        .dropDuplicates(["location_id"])
                        .filter(col("location_id").isNotNull()))
                        
        elif dim == "dim_aircraft":
            df_clean = (df_raw.withColumn("airline_owner", initcap(trim(col("airline_owner"))))
                        .withColumn("fleet_status", lower(trim(col("fleet_status"))))
                        .withColumn("total_flight_hours", col("total_flight_hours").cast("integer"))
                        .dropDuplicates(["tail_number"])
                        .filter(col("tail_number").isNotNull()))
                        
        elif dim == "dim_engines":
            df_clean = (df_raw.withColumn("engine_id", col("engine_id").cast("integer"))
                        .withColumn("model", trim(col("model")))
                        .withColumn("aircraft_type_supported", initcap(trim(col("aircraft_type_supported"))))
                        .withColumn("manufacture_date_clean", coalesce(
                            expr("try_to_date(manufacture_date, 'yyyy-MM-dd')"),
                            expr("try_to_date(manufacture_date, 'MM/dd/yyyy')")))
                        .drop("manufacture_date")
                        .withColumnRenamed("manufacture_date_clean", "manufacture_date")
                        .dropDuplicates(["engine_id"])
                        .filter(col("engine_id").isNotNull()))
                        
        elif dim == "dim_parts_bom":
            df_clean = (df_raw.withColumn("engine_id", col("engine_id").cast("integer"))
                        .withColumn("component_name", initcap(trim(col("component_name"))))
                        .withColumn("supplier_name", initcap(trim(col("supplier_name"))))
                        .withColumn("last_replaced_date", to_date(col("last_replaced_date"), "yyyy-MM-dd"))
                        .dropDuplicates(["part_id"])
                        .filter(col("part_id").isNotNull()))
                        
        elif dim == "dim_thresholds":
            df_clean = (df_raw.withColumn("sensor_name", lower(trim(col("sensor_name"))))
                        .withColumn("alert_description", initcap(trim(col("alert_description"))))
                        .withColumn("critical_limit", col("critical_limit").cast("double"))
                        .dropDuplicates(["sensor_name"])
                        .filter(col("sensor_name").isNotNull()))

        # Add Audit Column
        df_final = df_clean.withColumn("silver_processing_timestamp", current_timestamp())

        # WRITE & OPTIMIZE 
        try:
            initial_count = df_raw.count()
            final_count = df_final.count()

            df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
            spark.sql(f"OPTIMIZE {target_table}")

            print(f"    -> DQ: Read {initial_count} rows | Dropped {initial_count - final_count} invalid rows.")
            print(f"    -> Saved to {target_table}")
        except Exception as e:
            print(f"Failed writing {dim}. Error: {str(e)}")

if __name__ == "__main__":
    process_silver_dimensions(spark)
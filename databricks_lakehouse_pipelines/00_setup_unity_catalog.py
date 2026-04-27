"""
Infrastructure Setup: Unity Catalog Initialization
Purpose: Ensures the foundational Catalog and Medallion Schemas exist before any pipeline runs.
Execution: This only needs to be run ONCE when deploying the project to a new Databricks workspace.
"""

def setup_lakehouse_infrastructure(spark):
    print("Initializing Aerospace Lakehouse Infrastructure...")

    try:
        # 1. Create the top-level Catalog
        print("Checking Catalog...")
        spark.sql("CREATE CATALOG IF NOT EXISTS aerospace;")

        # Set the active catalog
        spark.sql("USE CATALOG aerospace;")
        print("Catalog 'aerospace' is ready.")

        # 2. Create the Medallion Schemas
        print("Checking Medallion Schemas...")
        spark.sql("CREATE SCHEMA IF NOT EXISTS bronze;")
        spark.sql("CREATE SCHEMA IF NOT EXISTS silver;")
        spark.sql("CREATE SCHEMA IF NOT EXISTS gold;")
        
        print("Unity Catalog setup complete. The Lakehouse is ready for data pipelines!")

    except Exception as e:
        print(f"Error during setup: {str(e)}")
        #"Note: If you get a permission error, ensure your Databricks user has 'CREATE CATALOG' privileges, or simply use the 'hive_metastore' default catalog."

# Entry point for Databricks Job Execution
if __name__ == "__main__":
    setup_lakehouse_infrastructure(spark)
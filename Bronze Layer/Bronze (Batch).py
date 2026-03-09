# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC use catalog `brazilian_e-commerce_project`;
# MAGIC use schema bronze
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingesitng Data Into Bronze Layer (Batch)

# COMMAND ----------

# DBTITLE 1,Cell 2
catalog = "brazilian_e-commerce_project"
schema = "bronze"
volume = "bronze_batch_landing"

tables = [
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "customers",
    "products",
    "sellers",
    "geolocation",
    "product_category_translation"
]
 
for table in tables:
    landing_path = f"/Volumes/{catalog}/{schema}/{volume}/landing/{table}"

    print(f"Ingesting {table}...")

    df = (spark.read.format("csv")\
          .options(header='true', inferSchema='true')\
          .load(landing_path))

    df.write.format("delta")\
     .mode("overwrite")\
     .option("mergeSchema", "true").saveAsTable(f"`{catalog}`.`{schema}`.`{table}_raw`")
        
    print(f"{table} loaded successfully.")
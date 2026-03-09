# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG `brazilian_e-commerce_project`;
# MAGIC USE SCHEMA bronze

# COMMAND ----------

catalog = "brazilian_e-commerce_project"
schema = "bronze"
volume = "bronze_batch_landing"

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Streaming Tables (Auto Loader)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Orders Table

# COMMAND ----------

orders_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_schema/orders/")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(f"/Volumes/{catalog}/{schema}/{volume}/landing/orders")
)

(
    orders_stream.writeStream
    .format("delta")
    .option("checkpointLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_checkpoints/orders/")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table("bronze.orders_raw")
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2) Custemers Table

# COMMAND ----------

customers_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_schema/customers/")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(f"/Volumes/{catalog}/{schema}/{volume}/landing/customers")
)

(
    customers_stream.writeStream
    .format("delta")
    .option("checkpointLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_checkpoints/customers/")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table("bronze.customers_raw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3)Order Payment Table

# COMMAND ----------

order_payments_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_schema/order_payments/")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(f"/Volumes/{catalog}/{schema}/{volume}/landing/order_payments")
)

(
    order_payments_stream.writeStream
    .format("delta")
    .option("checkpointLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_checkpoints/order_payments/")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table("bronze.order_payments_raw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##4) Order Items Table

# COMMAND ----------

order_items_stream= (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_schema/order_items/")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(f"/Volumes/{catalog}/{schema}/{volume}/landing/order_items")
)

(
    order_items_stream.writeStream
    .format("delta")
    .option("checkpointLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_checkpoints/order_items/")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table("bronze.order_items_raw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##5) Products 

# COMMAND ----------

product_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_schema/products/")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(f"/Volumes/{catalog}/{schema}/{volume}/landing/products")
)

(
    product_stream.writeStream
    .format("delta")
    .option("checkpointLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_checkpoints/products/")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table("bronze.products_raw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##6) Sellers Table

# COMMAND ----------

order_payments_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_schema/sellers/")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(f"/Volumes/{catalog}/{schema}/{volume}/landing/sellers")
)

(
    order_payments_stream.writeStream
    .format("delta")
    .option("checkpointLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_checkpoints/sellers/")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table("bronze.sellers_raw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##7) reviews

# COMMAND ----------

reviews_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_schema/order_reviews/")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(f"/Volumes/{catalog}/{schema}/{volume}/landing/order_reviews")
)

(
    reviews_stream.writeStream
    .format("delta")
    .option("checkpointLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_checkpoints/order_reviews/")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table("bronze.order_reviews_raw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##8) Geolocation Table

# COMMAND ----------

order_payments_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_schema/geolocation/")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(f"/Volumes/{catalog}/{schema}/{volume}/landing/geolocation")
)

(
    order_payments_stream.writeStream
    .format("delta")
    .option("checkpointLocation", f"/Volumes/{catalog}/{schema}/{volume}/streaming_checkpoints/geolocation/")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table("bronze.geolocation_raw")
)
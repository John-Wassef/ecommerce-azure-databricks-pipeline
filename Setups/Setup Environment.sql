-- Databricks notebook source
SHOW CATALOGS

-- COMMAND ----------

USE CATALOG `brazilian_e-commerce_project`

-- COMMAND ----------

select current_catalog()

-- COMMAND ----------

select current_metastore()

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS e_commerce_project
URL "abfss://brazilian-e-commerce-project@johnsamirproject.dfs.core.windows.net/"
WITH (CREDENTIAL ecommerceproject)


-- COMMAND ----------

-- MAGIC %fs ls "abfss://brazilian-e-commerce-project@johnsamirproject.dfs.core.windows.net/"
-- MAGIC

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze
     MANAGED LOCATION 'abfss://brazilian-e-commerce-project@johnsamirproject.dfs.core.windows.net/bronze';  
CREATE SCHEMA IF NOT EXISTS silver
     MANAGED LOCATION 'abfss://brazilian-e-commerce-project@johnsamirproject.dfs.core.windows.net/silver';  
CREATE SCHEMA IF NOT EXISTS gold
     MANAGED LOCATION 'abfss://brazilian-e-commerce-project@johnsamirproject.dfs.core.windows.net/gold';  

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------

-- DBTITLE 1,Untitled
USE SCHEMA bronze;

CREATE EXTERNAL VOLUME IF NOT EXISTS bronze_batch_landing
  LOCATION 'abfss://brazilian-e-commerce-project@johnsamirproject.dfs.core.windows.net/bronze_volume'


-- COMMAND ----------

USE CATALOG `brazilian_e-commerce_project`;
USE SCHEMA bronze;

CREATE TABLE IF NOT EXISTS orders_raw USING DELTA;
CREATE TABLE IF NOT EXISTS order_items_raw USING DELTA;
CREATE TABLE IF NOT EXISTS order_payments_raw USING DELTA;
CREATE TABLE IF NOT EXISTS order_reviews_raw USING DELTA;
CREATE TABLE IF NOT EXISTS customers_raw USING DELTA;
CREATE TABLE IF NOT EXISTS products_raw USING DELTA;
CREATE TABLE IF NOT EXISTS sellers_raw USING DELTA;
CREATE TABLE IF NOT EXISTS geolocation_raw USING DELTA;
CREATE TABLE IF NOT EXISTS product_category_translation_raw USING DELTA;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog = "brazilian_e-commerce_project"
-- MAGIC schema = "bronze"
-- MAGIC volume = "bronze_batch_landing"
-- MAGIC
-- MAGIC base_path = f"/Volumes/{catalog}/{schema}/{volume}"
-- MAGIC
-- MAGIC # Main folders (ONLY FILE-RELATED)
-- MAGIC dbutils.fs.mkdirs(f"{base_path}/landing")
-- MAGIC dbutils.fs.mkdirs(f"{base_path}/checkpoints")
-- MAGIC dbutils.fs.mkdirs(f"{base_path}/schema")
-- MAGIC
-- MAGIC tables = [
-- MAGIC     "orders",
-- MAGIC     "order_items",
-- MAGIC     "order_payments",
-- MAGIC     "order_reviews",
-- MAGIC     "customers",
-- MAGIC     "products",
-- MAGIC     "sellers",
-- MAGIC     "geolocation",
-- MAGIC     "product_category_translation"
-- MAGIC ]
-- MAGIC
-- MAGIC # Landing + Schema folders
-- MAGIC for table in tables:
-- MAGIC     dbutils.fs.mkdirs(f"{base_path}/landing/{table}")
-- MAGIC     dbutils.fs.mkdirs(f"{base_path}/schema/{table}")
-- MAGIC
-- MAGIC # Streaming checkpoints (only transactional tables)
-- MAGIC streaming_tables = [
-- MAGIC     "orders",
-- MAGIC     "order_items",
-- MAGIC     "order_payments",
-- MAGIC     "order_reviews"
-- MAGIC ]
-- MAGIC
-- MAGIC for table in streaming_tables:
-- MAGIC     dbutils.fs.mkdirs(f"{base_path}/checkpoints/{table}_bronze")
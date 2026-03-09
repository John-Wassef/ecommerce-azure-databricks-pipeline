# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog `brazilian_e-commerce_project`

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Orders Table

# COMMAND ----------

status_data = [
    (1, "shipped"),
    (2, "canceled"),
    (3, "approved"),
    (4, "invoiced"),
    (5, "created"),
    (6, "delivered"),
    (7, "unavailable"),
    (8, "processing")
]

dim_status = spark.createDataFrame(
    status_data,
    ["order_status_id", "order_status"]
)

dim_status.write \
    .format("delta") \
    .mode("overwrite") \
.saveAsTable("silver.dim_order_status")

# COMMAND ----------

orders = spark.table(".bronze.orders_raw")

orders = orders.withColumn(
    "order_status_id",
    when(col("order_status") == "shipped", 1)
    .when(col("order_status") == "canceled", 2)
    .when(col("order_status") == "approved", 3)
    .when(col("order_status") == "invoiced", 4)
    .when(col("order_status") == "created", 5)
    .when(col("order_status") == "delivered", 6)
    .when(col("order_status") == "unavailable", 7)
    .when(col("order_status") == "processing", 8)
)

orders = orders.drop("order_status")
orders = orders.na.drop("any")


orders.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.orders_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC # 2) Cutomers Table

# COMMAND ----------

customers=spark.table("`brazilian_e-commerce_project`.bronze.customers_raw")

customers=customers.na.drop('any')

customers=customers.select('customer_unique_id','customer_zip_code_prefix','customer_city','customer_state')

cutomers=customers.dropDuplicates(["customer_unique_id"])

customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.customers_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3) GeoLocation Table

# COMMAND ----------

geolocation=spark.table('bronze.geolocation_raw')

geolocation=geolocation.na.drop('any')

geolocation.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.geolocation_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4) Orders Items Table

# COMMAND ----------

orders_items=spark.table('bronze.order_items_raw')

orders_items.na.drop('any')

orders.dropDuplicates(["order_id"])

orders_items.write\
    .format('delta')\
    .mode('overwrite')\
    .saveAsTable("silver.orders_items_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC # 5) Order Payment Table

# COMMAND ----------

payment_types = [
    Row(payment_type_id=1, payment_type="boleto"),
    Row(payment_type_id=2, payment_type="not_defined"),
    Row(payment_type_id=3, payment_type="credit_card"),
    Row(payment_type_id=4, payment_type="voucher"),
    Row(payment_type_id=5, payment_type="debit_card")
]

payment_lookup_df = spark.createDataFrame(payment_types)

payment_lookup_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.payment_type_lookup")

# COMMAND ----------

payment = spark.table("bronze.order_payments_raw")

payment =payment.na.drop('any')

   
payment=payment.withColumn(
        "payment_type_id",
        when(col("payment_type") == "boleto", 1)
        .when(col("payment_type") == "not_defined", 2)
        .when(col("payment_type") == "credit_card", 3)
        .when(col("payment_type") == "voucher", 4)
        .when(col("payment_type") == "debit_card", 5)
    ).drop("payment_type")
    

payment.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.payment_clean")
    

# COMMAND ----------

# MAGIC %md 
# MAGIC # 6) Order Reviews Table

# COMMAND ----------

reviews = spark.table("bronze.order_reviews_raw")

reviews.withColumn(
        "review_comment_title",
        when(col("review_comment_title").isNull(), lit("no comment"))
        .otherwise(col("review_comment_title"))
    ).withColumn(
        "review_comment_message",
        when(col("review_comment_message").isNull(), lit("no comment"))
        .otherwise(col("review_comment_message"))
    )

reviews.dropna(subset=[
    "review_id",
    "order_id",
    "review_score"
])


reviews.withColumn(
        "review_creation_date",
        col("review_creation_date").cast(TimestampType())
    ).withColumn(
        "review_answer_timestamp",
        col("review_answer_timestamp").cast(TimestampType())
    )

reviews.dropDuplicates(["review_id"])


reviews.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.order_reviews_clean")

# COMMAND ----------

# MAGIC %md 
# MAGIC # 7) Products Catagory Translation Table

# COMMAND ----------

produtct_category = spark.table("bronze.product_category_translation_raw")

produtct_category.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.product_category_name_translation_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC # 8) Products Table

# COMMAND ----------

products= spark.table("bronze.products_raw")

products.na.drop('any')

products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.products_clean")

# COMMAND ----------

# MAGIC %md 
# MAGIC # 9) Seller Table

# COMMAND ----------

seller=spark.table("bronze.sellers_raw")

seller.na.drop('any')

seller.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.sellers_clean")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver.customers_clean CLUSTER BY (customer_unique_id);
# MAGIC ALTER TABLE silver.orders_items_clean CLUSTER BY (order_id);
# MAGIC ALTER TABLE silver.order_reviews_clean CLUSTER BY (review_id);
# MAGIC ALTER TABLE silver.orders_clean CLUSTER BY (order_id);
# MAGIC ALTER TABLE silver.payment_claen CLUSTER BY (order_id);
# MAGIC ALTER TABLE silver.products_clean CLUSTER BY (product_id);
# MAGIC ALTER TABLE silver.sellers_clean CLUSTER BY (seller_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE silver.customers_clean;
# MAGIC OPTIMIZE silver.orders_items_clean;
# MAGIC OPTIMIZE silver.order_reviews_clean;
# MAGIC OPTIMIZE silver.orders_clean;
# MAGIC OPTIMIZE silver.order_reviews_clean;
# MAGIC OPTIMIZE silver.payment_claen;
# MAGIC OPTIMIZE silver.sellers_clean;
-- Databricks notebook source
USE CATALOG `brazilian_e-commerce_project`;
USE SCHEMA GOLD;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating Dimentions Views

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1) Dim Cutomers

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_customers
USING DELTA
CLUSTER BY (customer_unique_id)
AS
SELECT
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM silver.customers_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2) Dim Products

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_products
USING DELTA
CLUSTER BY (product_id)
SELECT
    p.product_id,
    p.product_category_name,
    t.product_category_name_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM silver.products_clean p
LEFT JOIN silver.product_category_name_translation_clean t
ON p.product_category_name = t.product_category_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3) Dim Sellers

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_sellers 
USING DELTA
CLUSTER BY (seller_id)
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM silver.sellers_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4) Dim Payment Type

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_payment_type 
USING DELTA
CLUSTER BY(payment_type_id)
SELECT
    payment_type_id,
    payment_type
FROM silver.payment_type_lookup;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4) Dim Order Status

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_order_status
USING DELTA
CLUSTER BY(order_status_id)
SELECT *
FROM silver.dim_order_status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5) Dim Location

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_location 
USING DELTA
CLUSTER BY (geolocation_zip_code_prefix)
SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
FROM silver.geolocation_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating Fact Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1) Fact Orders

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_orders 
USING DELTA
CLUSTER BY (order_id)
SELECT
    o.order_id,
    o.customer_id,
    o.order_status_id,
    
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    
    o.order_estimated_delivery_date

FROM silver.orders_clean o;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2) Fact Order Items

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_order_items 
USING DELTA
CLUSTER BY (order_id)
SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    
    oi.price,
    oi.freight_value,
    
    (oi.price + oi.freight_value) AS total_item_value,

    oi.shipping_limit_date

FROM silver.orders_items_clean oi;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3) Fact Payments

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_payments
USING DELTA
CLUSTER BY (order_id)
SELECT
    p.order_id,
    p.payment_sequential,
    p.payment_type_id,
    p.payment_installments,
    p.payment_value
FROM silver.payment_clean p;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4) Fact Reviews

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_reviews 
USING DELTA 
CLUSTER BY (review_id)
SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM silver.order_reviews_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating Views

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1) View Product Sales

-- COMMAND ----------

CREATE OR REPLACE VIEW fct_product_sales AS
SELECT
    p.product_id,
    p.product_category_name_english,
    COUNT(oi.order_item_id) AS items_sold,
    SUM(oi.price) AS revenue
FROM fact_order_items oi
JOIN dim_products p
ON oi.product_id = p.product_id
GROUP BY
    p.product_id,
    p.product_category_name_english;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2) View Customer Orders

-- COMMAND ----------

CREATE OR REPLACE VIEW fct_customer_orders AS
SELECT
    c.customer_unique_id,
    c.customer_city,
    
    COUNT(DISTINCT o.order_id) AS total_orders,
    
    SUM(oi.price + oi.freight_value) AS total_spent

FROM fact_orders o
JOIN dim_customers c
ON o.customer_id = c.customer_unique_id

JOIN fact_order_items oi
ON o.order_id = oi.order_id

GROUP BY
    c.customer_unique_id,
    c.customer_city;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Optimizing Tables

-- COMMAND ----------

OPTIMIZE fact_payments;
OPTIMIZE fact_reviews;
OPTIMIZE fact_order_items;
OPTIMIZE fact_orders;

OPTIMIZE dim_customers;
OPTIMIZE dim_order_status;
OPTIMIZE dim_payment_type;
OPTIMIZE dim_location;
OPTIMIZE dim_products;
OPTIMIZE dim_sellers;
# 🛒 Brazilian E-Commerce Data Pipeline — Databricks + Azure

A end-to-end data engineering project built on **Databricks** with **Azure Data Lake Storage Gen2**, implementing a **Medallion Architecture** (Bronze → Silver → Gold) to process the open-source [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

---

## 📦 Dataset

The dataset is publicly available on Kaggle and contains ~100k orders from 2016–2018 made at multiple marketplaces in Brazil. It includes information across:

| Table | Description |
|-------|-------------|
| `orders` | Order lifecycle and status |
| `order_items` | Items purchased per order |
| `order_payments` | Payment methods and values |
| `order_reviews` | Customer review scores and comments |
| `customers` | Customer location and identifiers |
| `products` | Product dimensions and category |
| `sellers` | Seller location data |
| `geolocation` | Brazilian zip code coordinates |
| `product_category_translation` | Portuguese → English category names |

---

## 🏗️ Architecture Overview

```
Azure Data Lake Storage Gen2 (ADLS)
        │
        ▼
  ┌─────────────────────────────────────────────────┐
  │               BRONZE LAYER                      │
  │  Raw ingestion via Batch (CSV → Delta) and      │
  │  Streaming (Auto Loader / cloudFiles)           │
  └─────────────────────────────────────────────────┘
        │
        ▼
  ┌─────────────────────────────────────────────────┐
  │               SILVER LAYER                      │
  │  Cleaned, deduplicated, typed, and enriched     │
  │  tables with surrogate keys and lookups         │
  └─────────────────────────────────────────────────┘
        │
        ▼
  ┌─────────────────────────────────────────────────┐
  │               GOLD LAYER                        │
  │  Star schema: Dimension & Fact tables           │
  │  Optimized with Delta CLUSTER BY + OPTIMIZE     │
  └─────────────────────────────────────────────────┘
```

---

## ☁️ Infrastructure Setup

**Cloud:** Microsoft Azure  
**Compute:** Databricks (Unity Catalog enabled)  
**Storage:** Azure Data Lake Storage Gen2  
**Catalog:** `brazilian_e-commerce_project` (Unity Catalog)

### Schemas & Storage Locations

| Schema | ADLS Path |
|--------|-----------|
| `bronze` | `abfss://brazilian-e-commerce-project@.../bronze` |
| `silver` | `abfss://brazilian-e-commerce-project@.../silver` |
| `gold` | `abfss://brazilian-e-commerce-project@.../gold` |

An **External Volume** (`bronze_batch_landing`) is used to stage raw CSV files before ingestion.

---

## 📂 Project Structure

```
notebooks/
├── Setup_Environment.sql         # Catalog, schemas, volume, and directory setup
├── Bronze (Batch).py             # Batch ingestion: CSV → Delta (_raw tables)
├── Bronze (Streaming).py         # Streaming ingestion: Auto Loader → Delta
├── Silver.py                     # Data cleaning, typing, deduplication, lookups
└── Gold.sql                      # Star schema: dimension & fact tables + views
```

---

## 🥉 Bronze Layer

Raw data is ingested from CSV files stored in the ADLS landing volume and written to Delta tables suffixed with `_raw`.

### Batch Ingestion (`Bronze (Batch).py`)
Reads all 9 CSV tables using `spark.read` with `inferSchema` and writes them as Delta tables with `overwrite` mode. Suitable for historical or full loads.

### Streaming Ingestion (`Bronze (Streaming).py`)
Uses **Databricks Auto Loader** (`cloudFiles`) for incremental, file-by-file ingestion. Each table has dedicated:
- A **schema location** for schema inference state
- A **checkpoint location** for stream progress tracking
- `trigger(availableNow=True)` for triggered (non-continuous) stream execution

**Tables streamed:** `orders`, `customers`, `order_payments`, `order_items`, `products`, `sellers`, `order_reviews`, `geolocation`

---

## 🥈 Silver Layer

The Silver layer (`Silver.py`) cleans and enriches the raw tables using PySpark, then writes them as Delta tables to the `silver` schema.

### Key Transformations

| Table | Transformations |
|-------|-----------------|
| `orders_clean` | Encodes `order_status` → surrogate `order_status_id`; drops nulls |
| `customers_clean` | Selects relevant columns; drops nulls and duplicates on `customer_unique_id` |
| `geolocation_clean` | Drops nulls |
| `orders_items_clean` | Drops nulls |
| `payment_clean` | Encodes `payment_type` → surrogate `payment_type_id`; drops nulls |
| `order_reviews_clean` | Fills null comment fields; casts timestamp columns; drops duplicates |
| `products_clean` | Drops nulls |
| `sellers_clean` | Drops nulls |
| `product_category_name_translation_clean` | Passed through as-is |

### Lookup / Dimension Tables Created in Silver

- `dim_order_status` — maps `order_status_id` ↔ `order_status` string
- `payment_type_lookup` — maps `payment_type_id` ↔ `payment_type` string

### Optimization

All Silver tables are clustered and optimized:
```sql
ALTER TABLE silver.orders_clean CLUSTER BY (order_id);
OPTIMIZE silver.orders_clean;
-- (and so on for all tables)
```

---

## 🥇 Gold Layer

The Gold layer (`Gold.sql`) implements a **star schema** using Delta tables clustered for query performance.

### Dimension Tables

| Table | Grain | Cluster Key |
|-------|-------|-------------|
| `dim_customers` | Unique customer | `customer_unique_id` |
| `dim_products` | Product (with English category) | `product_id` |
| `dim_sellers` | Seller | `seller_id` |
| `dim_payment_type` | Payment type lookup | `payment_type_id` |
| `dim_order_status` | Order status lookup | `order_status_id` |
| `dim_location` | Zip code geolocation | `geolocation_zip_code_prefix` |

### Fact Tables

| Table | Grain | Cluster Key |
|-------|-------|-------------|
| `fact_orders` | One row per order | `order_id` |
| `fact_order_items` | One row per order line item | `order_id` |
| `fact_payments` | One row per payment event | `order_id` |
| `fact_reviews` | One row per review | `review_id` |

### Analytical Views

| View | Description |
|------|-------------|
| `fct_product_sales` | Items sold and revenue per product/category |
| `fct_customer_orders` | Total orders and total spend per customer |

### Optimization
All Gold tables are OPTIMIZE'd after creation to compact Delta files for fast reads.

---

## 🚀 Getting Started

1. **Provision Azure resources** — Create an ADLS Gen2 storage account and container (`brazilian-e-commerce-project`), and set up a Databricks workspace with Unity Catalog.

2. **Create External Credential** — Configure a storage credential (`ecommerceproject`) in Databricks to access ADLS.

3. **Run `Setup_Environment.sql`** — Creates the catalog, schemas, external volume, empty Delta tables, and ADLS directory structure.

4. **Upload CSV files** to the corresponding landing folders:
   ```
   /Volumes/brazilian_e-commerce_project/bronze/bronze_batch_landing/landing/<table_name>/
   ```

5. **Run Bronze notebooks** — Use the Batch notebook for a full load or the Streaming notebook for incremental Auto Loader ingestion.

6. **Run `Silver.py`** — Cleans and transforms raw data into analysis-ready Silver tables.

7. **Run `Gold.sql`** — Builds the star schema dimension and fact tables and the analytical views.

---

## 🛠️ Tech Stack

- **Databricks** (Unity Catalog, Delta Lake, Auto Loader)
- **Apache Spark / PySpark**
- **Azure Data Lake Storage Gen2**
- **Delta Format** with `CLUSTER BY` + `OPTIMIZE`
- **SQL** (Databricks SQL)

---

## 📄 License

Dataset: [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) — Olist Brazilian E-Commerce Public Dataset  
Code: MIT
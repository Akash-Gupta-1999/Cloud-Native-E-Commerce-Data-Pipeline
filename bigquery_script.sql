-- To Create a new schema in Bigquery

CREATE SCHEMA `gcp-project.olist_lakehouse`
OPTIONS(
  location = "US"
);

-- To create a new table in Bigquery with only metadata and data will be read from external source silver layer 

CREATE EXTERNAL TABLE `gcp-project.olist_lakehouse.olist_silver_data`
OPTIONS (
  format = 'PARQUET', -- Can also be PARQUET, AVRO, JSON
  uris = ['gs://otis-dataset-project/Silver/*.parquet']
);

-- to read data from the external table created above

select * from `olist_lakehouse.olist_silver_data` limit 10;

-- now creating a gold layer schema and creating a view on top of the silver layer to read data from silver layer and write it to gold layer

CREATE SCHEMA `gcp-project.olist_lakehouse_gold`
OPTIONS(
  location = "US"
);

-- creating a view on top of the silver layer to read data from silver layer and write it to gold layer for orders data

CREATE OR REPLACE VIEW `gcp-project.olist_lakehouse_gold.orders_gold_view` AS
SELECT
    order_id,
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    customer_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    product_category_name_english,
    product_weight_g,
    product_length_cm,
    product_description_lenght,
    price,
    freight_value,
    review_score,
    Delay_Time,
    actual_delivery_time,
    estimated_delivery_time,
    order_status,
    DATE(order_purchase_timestamp) AS order_date
FROM `olist_lakehouse.olist_silver_data`;

-- exporting data from gold layer to google cloud storage in parquet format

EXPORT DATA OPTIONS(
  uri='gs://otis-dataset-project/Gold/orders/*.parquet',
  format='PARQUET',
  overwrite=true
)
AS
SELECT *
FROM `gcp-project.olist_lakehouse_gold.orders_gold_view`;
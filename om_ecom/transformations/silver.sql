create streaming table om_silver.sales_pipeline_cleaned 
(
constraint valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
select * from stream sales_pipeline;

-- Create and populate the target table for scd1.
CREATE OR REFRESH STREAMING TABLE om_silver.products_scd1;

CREATE FLOW productsflow AS AUTO CDC INTO
  om_silver.products_scd1
FROM
  stream(saas.om_bronze.products_pipeline)
KEYS
  (product_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  seqNum
COLUMNS * EXCEPT
  (operation, seqNum, ingestion_date, _rescued_data)
STORED AS
  SCD TYPE 1;

-- Create and populate the target table for scd2.
CREATE OR REFRESH STREAMING TABLE om_silver.customers_scd2;

CREATE FLOW customersflow AS AUTO CDC INTO
  om_silver.customers_scd2
FROM
  stream(saas.om_bronze.customers_pipeline)
KEYS
  (customer_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
COLUMNS * EXCEPT
  (operation, sequenceNum, ingestion_date, _rescued_data)
STORED AS
  SCD TYPE 2;

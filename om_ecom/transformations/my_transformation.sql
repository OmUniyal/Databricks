-- s3://jpmctraining/pipeline_input/sales

-- ingesting sales data
create streaming table sales_pipeline AS
select *, current_date as ingestion_date from STREAM read_files(
  's3://jpmctraining/pipeline_input/sales',
  format => 'csv'
);


-- ingesting products data
create streaming table products_pipeline AS
select *, current_date as ingestion_date from STREAM read_files(
  's3://jpmctraining/pipeline_input/products',
  format => 'csv'
);

-- ingesting customers data
create streaming table customers_pipeline AS
select *, current_date as ingestion_date from STREAM read_files(
  's3://jpmctraining/pipeline_input/customers',
  format => 'csv'
);
CREATE TABLE enterprise_dw.dim_customer (
  customer_id STRING,
  name STRING,
  region STRING,
  created_date DATE
)
PARTITION BY created_date;

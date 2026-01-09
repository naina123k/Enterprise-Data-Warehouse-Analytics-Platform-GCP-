CREATE TABLE enterprise_dw.fact_sales (
  order_id STRING,
  customer_id STRING,
  product STRING,
  amount FLOAT64,
  order_timestamp TIMESTAMP
)
PARTITION BY DATE(order_timestamp)
CLUSTER BY customer_id;

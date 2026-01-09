from google.cloud import bigquery

client = bigquery.Client()

query = """
CREATE OR REPLACE TABLE enterprise_dw.sales_summary AS
SELECT
  customer_id,
  COUNT(order_id) AS total_orders,
  SUM(amount) AS total_spent
FROM enterprise_dw.fact_sales
GROUP BY customer_id
"""

client.query(query).result()

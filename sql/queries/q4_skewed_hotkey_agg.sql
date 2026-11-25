SELECT
  customer_id,
  COUNT(*) AS order_cnt,
  SUM(total_price) AS total_spend,
  AVG(total_price) AS avg_spend,
  MAX(total_price) AS max_order
FROM orders
WHERE order_status != 'X'
GROUP BY customer_id
ORDER BY order_cnt DESC, customer_id ASC
LIMIT 15

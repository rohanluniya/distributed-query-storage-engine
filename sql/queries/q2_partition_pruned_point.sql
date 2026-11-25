SELECT
  order_id,
  customer_id,
  order_status,
  order_month,
  region_id,
  total_price
FROM orders
WHERE order_month = '2025-03' AND region_id = 2 AND total_price >= 25000.0
ORDER BY total_price DESC, order_id ASC
LIMIT 25

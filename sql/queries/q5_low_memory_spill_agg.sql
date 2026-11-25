SELECT
  order_id,
  order_month,
  COUNT(*) AS line_cnt,
  SUM(extended_price) AS gross_revenue,
  SUM(quantity) AS total_qty
FROM lineitem
WHERE region_id BETWEEN 0 AND 2
GROUP BY order_id, order_month
ORDER BY gross_revenue DESC, order_id ASC
LIMIT 25

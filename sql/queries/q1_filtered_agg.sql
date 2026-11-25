SELECT
  return_flag,
  order_month,
  COUNT(*) AS line_cnt,
  SUM(quantity) AS total_qty,
  SUM(extended_price * (1 - discount)) AS net_revenue,
  AVG(extended_price) AS avg_price,
  MIN(quantity) AS min_qty,
  MAX(quantity) AS max_qty
FROM lineitem
WHERE ship_date <= '2025-05-31' AND discount >= 0.02
GROUP BY return_flag, order_month
ORDER BY return_flag ASC, order_month ASC

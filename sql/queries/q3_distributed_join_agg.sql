SELECT
  n.region_name,
  c.mkt_segment,
  COUNT(*) AS order_cnt,
  SUM(o.total_price) AS total_revenue,
  AVG(o.total_price) AS avg_order_value
FROM orders o
INNER JOIN customer c ON o.customer_id = c.customer_id
INNER JOIN nation n ON c.nation_id = n.nation_id
WHERE o.order_month IN ('2025-03', '2025-04') AND c.acct_bal > 1000.0
GROUP BY n.region_name, c.mkt_segment
ORDER BY total_revenue DESC, n.region_name ASC
LIMIT 20

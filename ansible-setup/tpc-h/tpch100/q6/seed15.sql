SELECT
  SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE
  l_shipdate >= CAST('1997-01-01' AS DATE)
  AND l_shipdate < CAST('1997-01-01' AS DATE) + INTERVAL '1' YEAR
  AND l_discount BETWEEN 0.05 - 0.01 AND 0.05 + 0.01
  AND l_quantity < 25
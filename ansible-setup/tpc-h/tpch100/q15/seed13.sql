SELECT
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
FROM supplier
JOIN (
  SELECT
    l_suppkey AS supplier_no,
    SUM(l_extendedprice * (
      1 - l_discount
    )) AS total_revenue
  FROM lineitem
  WHERE
    l_shipdate >= CAST('1995-04-01' AS DATE)
    AND l_shipdate < CAST('1995-04-01' AS DATE) + INTERVAL '3' MONTH
  GROUP BY
    l_suppkey
) AS revenue0
  ON s_suppkey = supplier_no
WHERE
  total_revenue = (
    SELECT
      MAX(total_revenue)
    FROM (
      SELECT
        l_suppkey AS supplier_no,
        SUM(l_extendedprice * (
          1 - l_discount
        )) AS total_revenue
      FROM lineitem
      WHERE
        l_shipdate >= CAST('1995-04-01' AS DATE)
        AND l_shipdate < CAST('1995-04-01' AS DATE) + INTERVAL '3' MONTH
      GROUP BY
        l_suppkey
    ) AS revenue_max
  )
ORDER BY
  s_suppkey
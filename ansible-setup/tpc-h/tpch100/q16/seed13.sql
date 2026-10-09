SELECT
  p_brand,
  p_type,
  p_size,
  COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM partsupp, part
WHERE
  p_partkey = ps_partkey
  AND p_brand <> 'Brand#15'
  AND NOT p_type LIKE 'SMALL POLISHED%'
  AND p_size IN (11, 17, 18, 16, 6, 37, 10, 49)
  AND NOT ps_suppkey IN (
    SELECT
      s_suppkey
    FROM supplier
    WHERE
      s_comment LIKE '%Customer%Complaints%'
  )
GROUP BY
  p_brand,
  p_type,
  p_size
ORDER BY
  supplier_cnt DESC,
  p_brand,
  p_type,
  p_size
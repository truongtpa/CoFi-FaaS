SELECT
  p_brand,
  p_type,
  p_size,
  COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM partsupp, part
WHERE
  p_partkey = ps_partkey
  AND p_brand <> 'Brand#24'
  AND NOT p_type LIKE 'MEDIUM PLATED%'
  AND p_size IN (8, 20, 4, 50, 13, 27, 40, 12)
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
SELECT
  SUM(l_extendedprice * (
    1 - l_discount
  )) AS revenue
FROM lineitem, part
WHERE
  (
    p_partkey = l_partkey
    AND p_brand = 'Brand#14'
    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND l_quantity >= 8
    AND l_quantity <= 8 + 10
    AND p_size BETWEEN 1 AND 5
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
  )
  OR (
    p_partkey = l_partkey
    AND p_brand = 'Brand#35'
    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    AND l_quantity >= 14
    AND l_quantity <= 14 + 10
    AND p_size BETWEEN 1 AND 10
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
  )
  OR (
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    AND l_quantity >= 22
    AND l_quantity <= 22 + 10
    AND p_size BETWEEN 1 AND 15
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
  )
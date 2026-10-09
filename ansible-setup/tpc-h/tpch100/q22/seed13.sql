SELECT
  cntrycode,
  COUNT(*) AS numcust,
  SUM(c_acctbal) AS totacctbal
FROM (
  SELECT
    SUBSTRING(c_phone, 1, 2) AS cntrycode,
    c_acctbal
  FROM customer
  WHERE
    SUBSTRING(c_phone, 1, 2) IN ('26', '21', '38', '24', '44', '31', '22')
    AND c_acctbal > (
      SELECT
        AVG(c_acctbal)
      FROM customer
      WHERE
        c_acctbal > 0.00
        AND SUBSTRING(c_phone, 1, 2) IN ('26', '21', '38', '24', '44', '31', '22')
    )
    AND NOT EXISTS(
      SELECT
        *
      FROM orders
      WHERE
        o_custkey = c_custkey
    )
) AS custsale
GROUP BY
  cntrycode
ORDER BY
  cntrycode
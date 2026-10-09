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
    SUBSTRING(c_phone, 1, 2) IN ('38', '27', '22', '42', '44', '35', '30')
    AND c_acctbal > (
      SELECT
        AVG(c_acctbal)
      FROM customer
      WHERE
        c_acctbal > 0.00
        AND SUBSTRING(c_phone, 1, 2) IN ('38', '27', '22', '42', '44', '35', '30')
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
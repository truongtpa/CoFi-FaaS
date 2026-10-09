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
    SUBSTRING(c_phone, 1, 2) IN ('21', '34', '27', '25', '37', '40', '38')
    AND c_acctbal > (
      SELECT
        AVG(c_acctbal)
      FROM customer
      WHERE
        c_acctbal > 0.00
        AND SUBSTRING(c_phone, 1, 2) IN ('21', '34', '27', '25', '37', '40', '38')
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
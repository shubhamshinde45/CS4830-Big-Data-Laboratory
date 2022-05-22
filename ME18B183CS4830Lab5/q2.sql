SELECT
  COUNT(*)
FROM
  bdl2022labs.irisdata.irisdatatable
WHERE
  Species='Iris-virginica'
  AND SepalWidth > 3.0
  AND PetalLength < 2.0
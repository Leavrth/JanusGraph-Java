SELECT CONCAT(
  "*3\r\n",
  '$',LENGTH(redis_cmd),'\r\n',redis_cmd,'\r\n',
  '$',LENGTH(redis_key),'\r\n',redis_key,'\r\n',
  '$',LENGTH(redis_value),'\r\n',redis_value,'\r'
)
FROM(
  SELECT
  'SET' as redis_cmd,
  CONCAT('Keyword:', vtxid) as redis_key,
  vid as redis_value
  FROM Keyword
) AS t

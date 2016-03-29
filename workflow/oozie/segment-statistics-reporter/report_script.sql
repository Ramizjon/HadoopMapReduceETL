use segments;

WITH segment_counts AS (
SELECT
key,
COUNT(value) AS occurences
FROM user_operations_parquet
LATERAL VIEW EXPLODE(segmentTimestamps) pair
AS key, value
WHERE
  year = ${year} AND
  month = ${month} AND
  day = ${day}
GROUP BY key)

INSERT OVERWRITE
DIRECTORY "/user/cloudera/tsv_reporter/${year}/${month}/${day}"
SELECT CONCAT_WS('\t', key, CAST(occurences AS string)) FROM segment_counts;

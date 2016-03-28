use segments;

CREATE EXTERNAL TABLE IF NOT EXISTS tsv_dump (
  segment string,
  occurences int)
PARTITIONED BY (
  year string,
  month string,
  day string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION "/user/cloudera/reporter_output/";

WITH segment_counts AS (
SELECT
key,
COUNT(value)
FROM user_operations_parquet
LATERAL VIEW EXPLODE(segmentTimestamps) pair
AS key, value
WHERE
year = ${year} AND
month = ${month} AND
day = ${day}
GROUP BY key)

INSERT INTO TABLE tsv_dump
PARTITION
 (year = ${year},
 month = ${month},
 day = ${day})
SELECT * FROM segment_counts;
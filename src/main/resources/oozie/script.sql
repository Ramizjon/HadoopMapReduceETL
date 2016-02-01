use segments;

CREATE EXTERNAL TABLE IF NOT EXISTS temp_users(
  items string
)
STORED AS TEXTFILE
LOCATION "${inputDir}";

CREATE TABLE IF NOT EXISTS user_operations(
  id string,
  operation string, 
  segments array<string>
)
PARTITIONED BY (
  year string,
  month string,
  day string,
  hour string
)
STORED AS PARQUET;

INSERT OVERWRITE TABLE user_operations
PARTITION (year = "${year}", month = "${month}", day = "${day}", hour = "${hour}")
SELECT 
      split(items,",")[0] id,
      split(items,",")[1] operation,
      split(regexp_extract(items,"([^\s]+),([^\s]+),(\w+)",0),",") segments
      FROM temp_users;
      
DROP TABLE temp_users;
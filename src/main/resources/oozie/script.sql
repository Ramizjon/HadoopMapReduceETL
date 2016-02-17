use segments;

CREATE EXTERNAL TABLE IF NOT EXISTS temp_users(
  items string
)
STORED AS TEXTFILE
LOCATION "${inputDir}";

CREATE TABLE IF NOT EXISTS user_operations(
  timestamp string,
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
PARTITION (year = ${year}, month = ${month}, day = ${day}, hour = ${hour})
SELECT
      split(items,",")[0] timestamp,
      split(items,",")[1] id,
      split(items,",")[2] operation,
      split(regexp_extract(items,"([a-zA-Z0-9_]+),([a-zA-Z0-9_]+),([a-zA-Z0-9_]+),(.*)",4),",") segments
      FROM temp_users;
      
DROP TABLE temp_users;
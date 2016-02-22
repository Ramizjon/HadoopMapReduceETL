use segments;

CREATE EXTERNAL TABLE IF NOT EXISTS temp_users(
  items string
)
STORED AS TEXTFILE
LOCATION "${inputDir}";

CREATE TABLE IF NOT EXISTS user_operations(
  timestamp string,
  user_id string,
  operation string,
  segments array<string>
)
PARTITIONED BY (
  year string,
  month string,
  day string,
  hour string,
  inout string
)
STORED AS PARQUET;

INSERT INTO TABLE user_operations
PARTITION (year = ${year}, month = ${month}, day = ${day}, hour = ${hour}, inout = "${input_output}")
SELECT
      split(items,",")[0] timestamp,
      split(items,",")[1] user_id,
      split(items,",")[2] operation,
      split(regexp_extract(items,"([a-zA-Z0-9_]+),([a-zA-Z0-9_]+),([a-zA-Z0-9_]+),(.*)",4),",") segments
      FROM temp_users;

DROP TABLE temp_users;
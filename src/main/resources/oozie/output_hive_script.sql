use segments;
drop table if exists user_operations_parquet;
drop table if exists temp_table;

create external table temp_table (
  userId string,
  command string,
  segments array<string>,
  timestamp string)
    ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
  STORED AS
    INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
    OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
    LOCATION "${outputDir}/out";

create external table user_operations_parquet (
  userId string,
  command string,
  segments array<string>,
  timestamp string)
 PARTITIONED BY (
  year string,
  month string,
  day string,
  hour string,
  inout string
)
  ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
  STORED AS
    INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
    OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
    LOCATION "${outputDir}/out";

  INSERT OVERWRITE TABLE user_operations_parquet
  PARTITION
 (year = ${year},
 month = ${month},
 day = ${day},
 hour = ${hour},
 inout = "${input_output}")
 SELECT * FROM temp_table;


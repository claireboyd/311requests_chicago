create external table ckboyd_zippop_csv(
  geography_type string,
  year string,
  geography string,
  total_population int)
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\"")
STORED AS TEXTFILE
  location 'ckboyd/zippop'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Run a test query to make sure the above worked correctly
select *  from ckboyd_zippop_csv limit 5;
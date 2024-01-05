-- First, map the CSV data we downloaded in Hive
create external table ckboyd_311requests_csv(
  sr_number string, 
  sr_type string,
  sr_short_code string,
  created_department string,
  owner_department string,
  status string,
  origin string, 
  created_date string,
  last_modified_date string,
  closed_date string,
  street_address string,
  city string,
  state string,
  zip_code string,
  street_number string,
  street_direction string,
  street_name string,
  street_type string,
  duplicate boolean,
  legacy_record boolean,
  legacy_st_number string,
  parent_st_number string,
  community_area tinyint,
  ward tinyint,
  electrical_district string,
  electricity_grid string,
  police_sector string,
  police_district string,
  police_beat string,
  precinct string,
  sanitation_division_days string,
  created_hour tinyint,
  created_day_of_week tinyint,
  created_month tinyint,
  x_coordinate decimal,
  y_coordinate decimal,
  latitude decimal,
  longitude decimal,
  location string)
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location 'ckboyd/requests';

-- Run a test query to make sure the above worked correctly
select sr_number,created_date,sr_type  from ckboyd_311requests_csv limit 5;

-- Create an ORC table for ontime data (Note "stored as ORC" at the end)
create table ckboyd_311requests(
  sr_number string, 
  sr_type string,
  sr_short_code string,
  created_department string,
  owner_department string,
  status string,
  origin string, 
  created_date string,
  last_modified_date string,
  closed_date string,
  street_address string,
  city string,
  state string,
  zip_code string, 
  street_number string,
  street_direction string,
  street_name string,
  street_type string,
  duplicate boolean,
  legacy_record boolean,
  legacy_st_number string,
  parent_st_number string,
  community_area tinyint,
  ward tinyint,
  electrical_district string,
  electricity_grid string,
  police_sector string,
  police_district string,
  police_beat string,
  precinct string,
  sanitation_division_days string,
  created_hour tinyint,
  created_day_of_week tinyint,
  created_month tinyint,
  x_coordinate decimal,
  y_coordinate decimal,
  latitude decimal,
  longitude decimal,
  location string,
  zip_code_cleaned string)
  stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table ckboyd_311requests 
select *, 
  CASE WHEN zip_code == "0" THEN "00000"
    WHEN zip_code == "" THEN "00000"
    WHEN zip_code REGEXP "[^0-9]" THEN "00000"
    ELSE zip_code
    END as zip_code_cleaned
from ckboyd_311requests_csv
    where sr_number is not null 
        and sr_type is not null
        and status is not null 
        and created_date is not null;

-- Run a test query to make sure the above worked correctly
select sr_number,created_date,sr_type,zip_code_cleaned  from ckboyd_311requests limit 5;
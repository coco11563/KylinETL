CREATE TABLE user_table(user_id String, gender String, user_name String) COMMENT 'USER DATA STORE' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ("hbase.columns.mapping" = " user_id:gender, user_id:user_name");

CREATE TABLE check_in_table(weibo_id String, pic_url String, geohash String,content String, json_file String, user_id String,unix_time TIMESTAMP,time_id String, city_id String, province_id String, country_id String, lat double, lon double) COMMENT 'CHECK IN DATA STORE' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ("hbase.columns.mapping" = " weibo_id:content#s,weibo_id:json_file#s,weibo_id:geohash#s, weibo_id:user_id#s, weibo_id:time_id#s ,weibo_id:city_id#s, weibo_id:province_id#s, weibo_id:country_id#s, weibo_id:unix_time,weibo_id:pic_url#s, weibo_id:lat#b,weibo_id:lon#b");
CREATE TABLE time_table(year String, month String, day String, time_id String, is_holiday String, date_time Date) COMMENT 'TIME STORE' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ("hbase.columns.mapping" = "time_id:year,time_id:month, time_id:day, time_id:is_holiday,time_id:date_time");

CREATE TABLE province_table(province_id String, province_name String) COMMENT 'PROVINCE DATA' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ("hbase.columns.mapping" = "province_id:province_name ");

CREATE TABLE city_table(city_id String, city_name String) COMMENT 'CITY DATA' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ("hbase.columns.mapping" = "city_id: city_name");

CREATE TABLE country_table(country_id String, country_name String) COMMENT 'COUNTRY DATA' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ("hbase.columns.mapping" = "country_id:country_name");

# data into HDFS
hdfs dfs -mkdir ckboyd/requests
hdfs dfs -mkdir ckboyd/zippop

hdfs dfs -put ckboyd_311requests.csv ckboyd/ckboyd_311requests.csv
hdfs dfs -put ckboyd_zippop.csv ckboyd/ckboyd_zippop.csv
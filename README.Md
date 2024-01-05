### MPCS 53014: Big Data Application Architecture
#### Final Project, Claire Boyd (ckboyd)

##### Quick Start Guide to Run My App (all code used in video)

https://github.com/claireboyd/311requests_chicago/raw/main/ckboyd_finalproject_video.mp4

In order to run my app, first run my speed layer application by first ssh-ing into the cluster and creating a 8070:8070 tunnel:

```
ssh -i {PATH-TO-PPK-OR-PPM-FILE} -L 8070:ec2-3-131-137-149.us-east-2.compute.amazonaws.com:8070 hadoop@ec2-3-131-137-149.us-east-2.compute.amazonaws.com
```

Go to the right directory where my uber-jar is for my speed layer.

```
cd ckboyd/app/target
```

Once you are in the right folder, run the following line of code to start my spark application that streams new input from my kafka topic every 2 seconds and increments my serving layer in hbase.

```
spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamRequests uber-speed_layer-1.0-SNAPSHOT.jar b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092
```

Now that that spark is accepting new data from my streaming layer, let's launch my app.

In a seperate terminal, ssh into the webserver:

```
ssh -i {PATH-TO-PPK-OR-PPM-FILE} ec2-user@ec2-3-143-113-170.us-east-2.compute.amazonaws.com
```

Then, go to the right directory where my app is.

```
cd ckboyd/app
```

Now, launch the app with the following line of code:

```
node app.js 3087 ec2-3-131-137-149.us-east-2.compute.amazonaws.com 8070 b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092
```

You should see some information from my serving layer print out to the console - that means it's working.

Now, to view the application itself, in a browser visit: http://ec2-3-143-113-170.us-east-2.compute.amazonaws.com:3087/ for the main application and http://ec2-3-143-113-170.us-east-2.compute.amazonaws.com:3087/log-request for the speed layer application.

To test that the speed layer is working do the following: press submit with the defaults and you should see 13831 total requests for 60637 in 2021 and 17 total 311 Information Only Calls. If you go to the log-request.html page, and press submit with the defaults and go back to the main page and press submit again, this should increment up by 1 for both the total and the 311 only specific value. You will also see the data submit to the topic in your terminal that is open to the cluster.

Happy exploring!

##### Project Goal

I am really interested in how citizens interact with their government, so I wanted to investigate the rates at which Chicago residents 311 requests. 311 requests are requests to the city which vary a ton in scope. Some are simple calls to 311 asking for some information (can call 311 in order to check if there is alternate side parking on your street next week), while others are asking the city to respond to a particular need (pick up the trash on my block because the garbage collection did not happen when it was supposed to).

My goal was to create an app where Chicago residents could input a year and their zipcode and they would get back a summary of 311 requests that correspond to those two inputs. The app would display the total 311 requests in that zipcode and year as well as the ratio of requests per resident in their zip code (total requests / population in that zip code). The app would also display a table of all the different possible 311 requests, with a total count of 311 requests of that type as well as the frequency of that type of request in that zip code.

##### Data Ingestion

I used two core datasets for my app: (1) all 311 requests and (2) total population by zip code, both from the from the Chicago Data Portal. In order to ingest my data, I ssh-ed into the cluster navigated to `ckboyd/input` directory where I wanted to temporarily store my data.

```zsh
ssh -i ~/courses/big_data/ckboyd_mpcs53014.pem -L 8070:ec2-3-131-137-149.us-east-2.compute.amazonaws.com:8070 hadoop@ec2-3-131-137-149.us-east-2.compute.amazonaws.com

cd ckboyd
mkdir input
cd input
```

Once in the `ckboyd/input` folder I ran the code from two different bash scripts saved in the `/data_ingestion` directory of this zip file which pulls in both of datasets in csv formats to my input directory.

After I got my two datasets in the name node, I needed to push them over to HDFS. First I created a few new directories in hdfs using the following commands:

```bash
hdfs dfs -mkdir -p ckboyd/requests
hdfs dfs -mkdir -p ckboyd/zippop

hdfs dfs -put ckboyd_311requests.csv ckboyd/requests
hdfs dfs -put ckboyd_zippop.csv ckboyd/zippop

```

Check to see if it's successfully migrated- success!

```bash
[hadoop@ip-172-31-34-141 input]$ hdfs dfs -ls ckboyd
Found 2 items
drwxr-xr-x   - hadoop hdfsadmingroup          0 2023-12-03 18:19 ckboyd/requests
drwxr-xr-x   - hadoop hdfsadmingroup          0 2023-12-03 20:53 ckboyd/zippop
```

##### Batch Processing

Next, I read the data into HIVE using the OpenCSV Serde. I thought this was the easiest option given that each of my datasets were only one CSV, so this would be the most efficient process. After getting into hive using the below command, I submitted the queries in the `batch_processing` directory(`hive_requests.hql` and `hive_zippop.hql`) in order to create my original, ground-truth dataset in hive. 

```bash
beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver
```

After doing a bunch of data exploration by querying these two datasets, I realized there were some inconsistencies with the zip code data. Because I want my user to be able to sort by zip code and year, I knew that my hbase key needed to be a concatenation of these two fields. When I concatenate these two inputs together, they need to be uniform in length so that I can parse out the different components later.

The input data had some zip code values that were not 5 characters long so I did a very simple cleaning step in **hive_requests.hql** in order to clean the zip codes in the request data making the following decisions:
- keep original "zip_code" but make a new "zip_code_cleaned"
- save all "0" or "" zip codes to "00000"
- save all non-uniform zip codes (e.g. "G AVE") to "00000"

Next, I created the following batch views to summarize my data (all queries available in **batch_views.hql**):
1. **ckboyd_total_count_pop_summary**: This table summaries all my 311 requests (ckboyd_311requests) into total counts by zip code and year, and merges the population data (ckboyd_zippop_csv) so that each row contains a zip code, year, total_count, and total_population.
2. **ckboyd_requests_freq**: This table summarizes all my 311 requests (ckboyd_311requests) into total counts by EACH service request type, so it groups by zip, year, and sr_short_code (the variable corresponding to the unique service request type).
3. **ckboyd_requests_summary**: This is the most important batch view, as it pivots the previous batch view to a wide dataset, creating new columns that capture the counts of each of the 107 service requests as count_{sr_short_code}. This is a really chunky and hacky approach to pivoting the dataset that I wish I had more time to make a bit more streamlined. In my understanding, HIVE doesn't have a pivot function that would allow me do this a bit more easily so I had to do this manually. However, I created small for loops (example in **case_when_query.py**) to help me write the code for this to make it a bit less labor-intensive. In the end, this table has a row for each zip code/year combination with counts for each service request, total count and total population as columns.
4. **ckboyd_request_name_crosswalk**: Lastly, this batch view is a crosswalk of service request codes (sr_short_code) with the full name of the service request (sr_type). My goal was to use this at the end to map the values of the codes back to longer service request names.

The **batch_views.hql** script includes samples of what these outputs look like for each of these batch views.

##### Serving Layer

Next, we need to get the last two batch views into hbase in order to use in our front end application. First I need to create two hbase tables to put all the HIVE data into.

```
hbase shell
create 'ckboyd_request_summary','stats'
create 'ckboyd_request_crosswalk','name'
```

Next, I can go back into HIVE and run all of the code in **write_to_hbase** to save all my data in hbase.

```
beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver
```

I can check to see if HIVE table is now in hbase for ckboyd_request_summary - which it is!

```
hbase:002:0>  get 'ckboyd_request_summary', "606372021", "stats:count_311IOC"
COLUMN                                         CELL                                                                                                                                   
 stats:count_311IOC                            timestamp=2023-12-08T17:16:28.749, value=\x00\x00\x00\x00\x00\x00\x00\x12                                                              
1 row(s)
Took 0.0284 seconds                            
```


##### Speed Layer

To create my speed layer, first I need to create a topic in kafka to hold the data I want to use to increment my hbase table. First, I SSH into the cluster.

```
ssh -i ~/courses/big_data/ckboyd_mpcs53014.pem hadoop@ec2-3-131-137-149.us-east-2.compute.amazonaws.com
```

Get to the right directory:

```
cd /home/hadoop/kafka_2.12-2.8.1/bin
```

Then check the existing topics in kafka:

```
kafka-topics.sh --list --zookeeper z-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:2181,z-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:2181,z-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:2181
```

Great it's displaying a bunch of topics. Let's add mine.
```
kafka-topics.sh --create --zookeeper z-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:2181,z-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:2181,z-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:2181 --replication-factor 1 --partitions 1 --topic ckboyd_new311requests'

### result
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic ckboyd_new311requests.
```

Great - let's make sure it exists in our list of topics.

```
kafka-topics.sh --list --zookeeper z-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:2181,z-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:2181,z-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:2181
```

After using my log request form (more in this in the app section) to submit a bunch of new topics, let's display the contents of my topic:

```
kafka-console-consumer.sh --bootstrap-server b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092 --topic ckboyd_new311requests --from-beginning

#results
[hadoop@ip-172-31-34-141 bin]$ kafka-console-consumer.sh --bootstrap-server b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092 --topic ckboyd_new311requests --from-beginning
{"year":"2021","zip_code_cleaned":"60637","sr_short_code":"HFB"}
{}
{}
{}
{}
{"year":"2021","zip_code_cleaned":"60637","sr_short_code":"HFB"}
^CProcessed a total of 6 messages
```

Now, I need to create a scala program that takes my data and increments my hbase table correctly. Using the speed-layer-weather-archetype, I wrote two scala scripts (in the `speed_layer/src/main/scala`) that did the following:
* RequestsRecord: Sets the schema for what to expect in the kafka topic.
* StreamRequests: Every two seconds, it looks at each entry in the kafka topic (see above) and after connecting to the hbase table ckboyd_request_summary it finds the row with the key concat(zip_code_cleaned, year) and adds one to the total as well as the count_{sr_short_code}.

After finishing these scripts, I maven installed to build an uber-jar and then deployed it to my `ckboyd/app` directory in our cluster. To test it to make sure it was looking at my topic and incrementing correctly, I ran the following line of code in hadoop (that same directory):
```
spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamRequests uber-speed_layer-1.0-SNAPSHOT.jar b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092
```

It's working!
```
-------------------------------------------
Time: 1701998366000 ms
-------------------------------------------

-------------------------------------------
Time: 1701998368000 ms
-------------------------------------------

-------------------------------------------
Time: 1701998370000 ms
-------------------------------------------

-------------------------------------------
Time: 1701998372000 ms
-------------------------------------------
Updated speed layer for request HFB in zip60637 in 2021

-------------------------------------------
Time: 1701998374000 ms
-------------------------------------------
```

##### App

The main app has the same file structure as app_with_weather_form and a few core files to note:
* app.js: core application file that takes input from the form (zip_code and year), and returns a few summary stats as well as the table with all of the counts and freqencies of the hbase table corresponding to the zipyear.
* index.html: the html layout of the main page of the app. 
* log-request.html: the html layout of the speed layer input of the app.

The rest are just .css files or other templates for formatting the html.

The core difference here is that I looped through a list of the strings of all the possible sr_short_code in order to populate a list to use to build our table in mustache. The biggest difficulty I had was trying to figure out how to access the second hbase table in order to use the request name instead of the request code. You can see the code that was getting the right values but not saving correctly to the output array in order to be accessed in my mustache table. I tried nesting it as the professor modeled on that Ed post, but my structure was a bit different because I needed a response from the hbase table for every item in my loop, not just one row.

See above (quick start) for the step by step of how to run the app.

##### Things I would do differently

If I had more time and had thought about the database design a bit more, I would have done a few things differently:
* **I would figure out how to pull in the names of the requests.** From a user perspective, the sr_short_codes don't mean anything but I spent awhile trying to figure out another way to use sr_type instead of sr_short_code but the spaces made it hard to do that and then get back the same values afterward.
* **I would not figured out a more elegant way to create all the count_{sr_short_code}.** My hive query is super clunky and I feel like there must be a better way to accomplish what I wanted to do besides naming 107 variables multiple times.
* **I would figure out a way to model or impute missing population data.** I realized towards the end of the project that I only have population data until 2021, so I would fill in those missing values somehow (either by tracking down a new dataset or modeling it).

Thanks!
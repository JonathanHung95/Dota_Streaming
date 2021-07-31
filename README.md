# Dota_Streaming
Project to demonstrate an end-to-end streaming pipeline for data engineering purposes.  I aim to stream data from the Dota API using NiFi, feed the data to a Change Data Capture (CDC) database, perform data transformations via Spark Streaming and then write to a table to be utilized by a front end dashboard.

![Flow Diagram](assets/flow_diagram.png)

### Dota 2 API calls

Data can be obtained from the Dota 2 API using HTTP requests.  In this case, I use 2 HTTP requests; the first is to pull the latest match id from the API and the second is to use the match id to pull the full match details.  I also do 2 one-off requests to pull the hero and item data.

The API will return a JSON object of the data in both cases (data examples can be found under the data folder).

### NiFi

![NiFi Flow](assets/nifi_flow.png)

I use NiFi to ingest data from the Dota API and direct it to our CDC database (a mysql database in this case).  Note that NiFi is set to pull data once every 2 seconds here.  This is because Steam limits API calls to 100,000 a day and since I need to make 2 calls to the API, I can't call at a higher rate.

Additionally, I use a JOLT JSON Transformer in order to hammer the JSON object into something that fits the database better.  This includes removing data that I'm not interested in and transforming the single JSON into an array of 10 entries, each corresponding to a different player from the match.  This allows me to use the Split JSON processor to split the JSON into 10 entries before it hits the Convert JSON to SQL and Put SQL processors.

The latter 2 processors will convert the JSON to an INSERT SQL query and put all 10 entries into the CDC.

### Change Data Capture (CDC) --MySQL

The CDC contains all the raw data ingested and transformed by NiFi.  From here, Debezium is used to link the database to a Kafka cluster that ingests the data as a stream.  

Some specifics:
1. Database: dota
2. Table: match_data

### Spark Streaming

The spark job is set up to handle the streaming data from kafka and do a few simple transformations and then output to Hudi and Postgresql.

Transformations
1. Convert hero and item ids into the actual names
2. Convert the unix time stamp into a human-readable time stamp
3. Convert the radiant_win column into a neutral win value (ie. not depending on the team)

### Superset

I use Superset to make a simple dashboard to analyze the data as it comes in.  It's set to auto-refresh every 30 seconds, allowing it to display data in a more real-time manner.

![Superset Dashboard](assets/dashboard.png)

### Setup

Roughly how to set-up the project.  

Clone the repository into your EC2 instance and run:

```
wget http://www.java2s.com/Code/JarDownload/mysql/mysql-connector-java-5.1.17-bin.jar.zip
unzip mysql-connector-java-5.1.17-bin.jar.zip 
docker-compose up
```

This will download the JDBC connector to the /home/ directory which is mounted to the NiFi image in the docker-compose.

You can access NiFi on http://EC_2_IP:8080/nifi/ .  Configure the JDBC connector to /opt/nifi/nifi-current/custom-jar/mysql-connector-java-8.0.25 so that NiFi can write to the MySQL CDC.  Load the Dota_Streaming_Template.xml into NiFi; this will give the full template used.  You may have to edit the api_key in the GetConfig processor.

Run:

```
docker exec -it mysql bash
```

To access the mysql container and set up the necessary database and table.  Table code can be found in dota_mysql.sql.  To deploy the debezium connector:

```
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{ "name": "inventory-connector", "config": { "connector.class": "io.debezium.connector.mysql.MySqlConnector", "tasks.max": "1", "database.hostname": "mysql", "database.port": "3306", "database.user": "root", "database.password": "debezium", "database.server.id": "184054", "database.server.name": "dbserver1", "database.include.list": "dota", "database.history.kafka.bootstrap.servers": "kafka:9092", "database.history.kafka.topic": "dbhistory.dota" } }'
```

To check the connectors:

```
curl -H "Accept:application/json" localhost:8083
curl -H "Accept:application/json" localhost:8083/connectors/
```

To check that kafka is ingesting data:

```
docker exec -it kafka bash
bin/kafka-console-consumer.sh  --topic dbserver1.dota.match_data --bootstrap-server CONTAINER_IP:9092
```

To set up the Postgresql database:

```
docker exec -it postgres bash
psql -h localhost -p 5432 -U postgres -W
```

The sql for the postgres table can be found in the src folder.

To run the jar in the spark job:

```
cp /Dota_Streaming/data/test2.json .
docker exec -it spark-master bash
spark/bin/spark-submit jars/dota_spark_streaming_2.12.8-1.0.jar --class dota_streaming.streaming.StreamingJob
```

This will copy the test2.json file to the home directory so that the spark job can use it to infer the schema for the data ingested from kafka. Run the spark job to process the data and write to Hudi and Postgresql database.

Superset can be accessed at http://EC2_IP:8081/admin/.  From here, you can make dashboards and/or illustrate the data.


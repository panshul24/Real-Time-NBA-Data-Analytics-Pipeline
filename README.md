# Real-Time-NBA-Data-Analytics-Pipeline

Problem Statement:
In professional basketball, particularly within the NBA, real-time data analytics are pivotal for enhancing game
analysis, optimizing player performance, and boosting fan engagement. Despite the potential of live data, its effective
utilization is often hampered by the complexity of processing and visualizing this data in real-time. Our project
accesses live NBA data using the nba_api, which provides a rich set of game statistics through endpoints
like https://nba-prod-us-east-1-mediaops-stats.s3.amazonaws.com/NBA/liveData/scoreboard/
todaysScoreboard_00.json. The API outputs data in JSON format, encapsulating game details such as scores, player
stats, and team performance metrics across various game periods.

However, challenges persist in swiftly ingesting, processing, and displaying these data points. The structure of the
data includes nested elements under keys like games, homeTeam, and awayTeam, detailing real-time updates
like game scores, individual player performances, and game status. The goal of our proposed data pipeline—
integrating Apache Kafka, Apache Spark, InfluxDB, and Grafana—is to streamline these processes. By setting
up Kafka for efficient data ingestion, using Spark for real-time data analytics, storing processed data in InfluxDB,
and visualizing insights through Grafana, we aim to facilitate quick, informed decision-making for coaches and
teams, and enhance the interactive experience for fans. This pipeline promises to revolutionize how stakeholders
interact with live game data, turning raw stats into actionable insights instantaneously.

Example Scoreboard made using the pipline :
![image](https://github.com/panshul24/Real-Time-NBA-Data-Analytics-Pipeline/assets/68370779/125d5982-a844-4a7a-b610-a246d4d5b397)


Task 1: Data Ingestion

Apache Kafka is the platform used for handling real-time data feeds. It is a distributed event streaming
platform capable of handling trillions of events a day.

Apache Kafka Requires Java to run, as Kafka itself is a Java-based system.

Install Java:
When setting up a data pipeline involving Apache Kafka, Java is a necessary dependency. Go to Oracle's official Java
download page. Choose the version of Java Development Kit (JDK) you need. For most users working with Kafka
and Spark, JDK 22 or the latest version will be suitable. Run the installer and check if the java has been installed or
not from the terminal.

echo $JAVA_HOME
java -version

Install Zookeeper:
Go to the Apache Zookeeper releases page to download the latest stable release of Zookeeper. Look for the binary
archive, which typically has an extension .tar.gz. Extract the file and have it ready.

Install Kafka:
Download Apache Kafka binary file Scala 2.13from the official website.

Once the file is downloaded place the file in a folder. Extract the file in the same folder by giving the following
command on the terminal.

tar -xzvf kafka_2.13-3.7.0.tar.gz

Kafka Setup:

Next objective is to create a virtual environment (venv) so that the Kafka server can be run in it.

After the successful creation of the environment venv navigate to the extracted Kafka folder in the venv.

Now we run the below script to launch the zookeeper which is required to run Kafka.

bin/zookeeper-server-start.sh config/zookeeper.properties

After the above command is run in the terminal successfully, below results will be displayed indicating the
zookeeper service is running

Note: Do not terminate the zookeeper terminal as we need to keep this service running

Next step is run a kafka server, open another terminal and navigate to extracted Kafka directory and give the below
command to run a script which will launch the Kafka server.

bin/kafka-server-start.sh config/server.properties

Once the command is run successfully, below results will be shown indicating Kafka server is now setup and is now
ready for data to be sent to it.

In an another terminal deactivate virtual environment venv, as this terminal will be later used to run a python file.

Create a python script in which you provide the data source and the Kafka topic for it to be written to.

The kafka-python library, which is a Kafka client for Python, allows for easy interaction with the Kafka system. This
library can be installed using below command.

pip install kafka-python

Below is the script which used to send NBA data from NBA api. This code also contains a scheduler which ensures
that the data is updated every 60 seconds in the Kafka topic/server. The scheduler can be removed as well depending
on the use case.

Code:

from kafka import KafkaProducer
import json
import time
from nba_api.live.nba.endpoints import scoreboard
from apscheduler.schedulers.blocking import BlockingScheduler
import logging

Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(name)

Kafka Configuration
kafka_broker = 'localhost:9092'
kafka_topic = 'nba-scoreboard-1' <topic name

Create a Kafka producer
producer = KafkaProducer(
bootstrap_servers=[kafka_broker],
value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def fetch_nba_scores():
try:

Fetch today's NBA scoreboard
games = scoreboard.ScoreBoard()
data = games.get_dict() # Fetch the games data as a dictionary
return data
except Exception as e:
logger.error(f"Failed to fetch NBA scores: {e}")
return None

def send_data_to_kafka(data):
try:
if data:
producer.send(kafka_topic, data)
producer.flush()
logger.info("Data sent to Kafka successfully.")
else:
logger.info("No data to send.")
except Exception as e:
logger.error(f"Failed to send data to Kafka: {e}")

def fetch_and_send():
data = fetch_nba_scores() # Fetch data from NBA API
send_data_to_kafka(data) # Send the data to Kafka

def main():
scheduler = BlockingScheduler()
scheduler.add_job(fetch_and_send, 'interval', minutes=1) # Adjust the interval as needed
try:
scheduler.start()
except (KeyboardInterrupt, SystemExit):
pass

if name == "main":
main()

Save the python file and run in the terminal as shown below.

Python kaf.py

Once the data is sent successfully you can see a print statement in the terminal:

INFO:__main__:Data sent to Kafka successfully.
Viewing the data that is sent to Kafka:
Open a new terminal and ensure that you are in the same venv and then navigate to the Kafka folder.
Use the below command to run a script that shows the data that is there in the Kafka topic from beginning.

bin/kafka-console-consumer.sh --topic <your_topic_name> --from-beginning --bootstrap-server localhost:

Below you can see a screenshot of the NBA data that was sent to Kafka.

This Completes Apache Kafka setup.

Task 2: Data Processing, and Storing

Setup Apache Spark:

PySpark is used to process the data streaming from Kafka. It handles reading, transforming, and extracting useful
information from the Kafka topics. The PySpark library in Python, installed from the below command.

pip install pyspark

Spark-Kafka connector allows PySpark to read from and write to Kafka topics directly. This is crucial for integrating
Kafka streams with Spark processing.
Spark-SQL-Kafka- 0 - 10 Package must be specified in your Spark session to connect Spark with Kafka. It’s included
with Spark but needs to be explicitly enabled with configuration options when initializing the Spark session:

.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka- 0 - 10_2.12:<spark_version>")

Replace <spark_version> with your actual Spark version.

Setup Influx DB:

InfluxDB is a time-series database designed to handle high write and query loads. It is particularly useful in scenarios
involving real-time analytics.
InfluxDB Client for Python: influxdb_client library, allows you to connect, write, and query data from an InfluxDB
instance. This can be installed using following command.

pip install influxdb-client

Creating a bucket in InfluxDB can be done through the InfluxDB UI (User Interface). Open your web browser and
navigate to the InfluxDB instance. The default URL is usually http://localhost:

Once Logged in we need to create and setup bucket in the influx db where the data is stored. As explained above
install the dependency if not installed earlier.

Create and save the token, later to be used to find the bucket

Now we create organization and set up server url as they are needed to have an initial connection, here ‘UIUC’ is the
name given to organization.

Click on create bucket, below are the 2 buckets created in this process, once done test data can be sent to check, and
below images shows the same setup

In the data explorer, flux query can be run to retrieve the data.

From this set up below are the things to be noted:
influxdb_token = "CGJpFEeqwj1XiZe0HXFyT38_UOCuxrX-qEhIx7RyA4KY8L5HlCN-
pvvdghxStvXuoN56RmrIfW0N8JGyU2B1MA=="

influxdb_org = "UIUC"

influxdb_bucket = "n_score"

influxdb_url = http://localhost:

Python Shell Code:
Once all the above set up is done, create a new python script which consumes the data from our kafka topic consumer
and defines schema for our data to convert from json to tabular format and send it to our influx db at every minute
interval, below is the code:

[Yesterday 9:23 PM] Saraswat, Panshul

Spark code :

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import *
from influxdb_client import InfluxDBClient, Point, WriteOptions

InfluxDB Client Configuration
url = "http://localhost:8086"
token = "CGJpFEeqwj1XiZe0HXFyT38_UOCuxrX-qEhIx7RyA4KY8L5HlCN-
pvvdghxStvXuoN56RmrIfW0N8JGyU2B1MA=="
org = "UIUC"
bucket = "n_score"
client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=WriteOptions(batch_size=1000, flush_interval=10_000))

Define the schema based on the nested JSON structure
schema = StructType([
StructField("meta", StructType([
StructField("version", IntegerType()),
StructField("request", StringType()),
StructField("time", StringType()),
StructField("code", IntegerType())
])),
StructField("scoreboard", StructType([
StructField("gameDate", StringType()),
StructField("leagueId", StringType()),
StructField("leagueName", StringType()),
StructField("games", ArrayType(StructType([
StructField("gameId", StringType()),
StructField("gameCode", StringType()),
StructField("gameStatus", IntegerType()),
StructField("gameStatusText", StringType()),
StructField("period", IntegerType()),
StructField("gameClock", StringType()),
StructField("gameTimeUTC", StringType()),
StructField("gameEt", StringType()),
StructField("regulationPeriods", IntegerType()),
StructField("ifNecessary", BooleanType()),
StructField("seriesGameNumber", StringType()),
StructField("gameLabel", StringType()),
StructField("gameSubLabel", StringType()),
StructField("seriesText", StringType()),
StructField("seriesConference", StringType()),

StructField("poRoundDesc", StringType()),
StructField("gameSubtype", StringType()),
StructField("homeTeam", StructType([
StructField("teamId", IntegerType()),
StructField("teamName", StringType()),
StructField("teamCity", StringType()),
StructField("teamTricode", StringType()),
StructField("wins", IntegerType()),
StructField("losses", IntegerType()),
StructField("score", IntegerType()),
StructField("seed", IntegerType()),
StructField("inBonus", StringType()),
StructField("timeoutsRemaining", IntegerType()),
StructField("periods", ArrayType(StructType([
StructField("period", IntegerType()),
StructField("periodType", StringType()),
StructField("score", IntegerType())
])))
])),
StructField("awayTeam", StructType([
StructField("teamId", IntegerType()),
StructField("teamName", StringType()),
StructField("teamCity", StringType()),
StructField("teamTricode", StringType()),
StructField("wins", IntegerType()),
StructField("losses", IntegerType()),
StructField("score", IntegerType()),
StructField("seed", IntegerType()),
StructField("inBonus", StringType()),
StructField("timeoutsRemaining", IntegerType()),
StructField("periods", ArrayType(StructType([
StructField("period", IntegerType()),
StructField("periodType", StringType()),
StructField("score", IntegerType())
])))
])),
StructField("gameLeaders", StructType([
StructField("homeLeaders", StructType([
StructField("personId", IntegerType()),
StructField("name", StringType()),
StructField("jerseyNum", StringType()),
StructField("position", StringType()),
StructField("teamTricode", StringType()),
StructField("playerSlug", StringType()),
StructField("points", IntegerType()),
StructField("rebounds", IntegerType()),
StructField("assists", IntegerType())
])),
StructField("awayLeaders", StructType([
StructField("personId", IntegerType()),
StructField("name", StringType()),
StructField("jerseyNum", StringType()),
StructField("position", StringType()),
StructField("teamTricode", StringType()),
StructField("playerSlug", StringType()),
StructField("points", IntegerType()),

StructField("rebounds", IntegerType()),
StructField("assists", IntegerType())
]))
])),
StructField("pbOdds", StructType([
StructField("team", StringType()),
StructField("odds", DoubleType()),
StructField("suspended", IntegerType())
]))
])))
]))
])

Initialize Spark Session
spark = SparkSession.builder.appName("NBA Scores Kafka Consumer").getOrCreate()

Read from Kafka
df = spark.readStream.format("kafka")
.option("kafka.bootstrap.servers", "localhost:9092")
.option("subscribe", "nba-scoreboard-1")
.load()

Select message value and convert from bytes to string
df = df.selectExpr("CAST(value AS STRING)")

Parse the JSON string using the defined schema
parsed_df = df.select(from_json(col("value"), schema).alias("data"))

Explode the games array into individual game records
games_df = parsed_df.select(explode(col("data.scoreboard.games")).alias("game"))

Select relevant fields for processing
flattened_df = games_df.select(
col("game.gameId").alias("gameId"),
col("game.gameCode").alias("gameCode"),
col("game.gameStatusText").alias("gameStatusText"),
col("game.homeTeam.teamName").alias("homeTeamName"),
col("game.homeTeam.score").alias("homeScore"),
col("game.awayTeam.teamName").alias("awayTeamName"),
col("game.awayTeam.score").alias("awayScore"),
col("game.period").alias("period"),

col("game.gameTimeUTC").alias("gameTimeUTC")
)

Function to write each batch of DataFrame to InfluxDB
def write_to_influx(batch_df, epoch_id):
points = []
for row in batch_df.collect():
point = Point("nba_games")
.tag("gameId", row.gameId)
.tag("gameCode", row.gameCode)
.field("gameStatusText", row.gameStatusText)
.field("homeTeamName", row.homeTeamName)
.field("homeScore", int(row.homeScore))
.field("awayTeamName", row.awayTeamName)
.field("awayScore", int(row.awayScore))
.field("period", int(row.period))
.time(row.gameTimeUTC)
points.append(point)
write_api.write(bucket=bucket, record=points)

Write the stream to InfluxDB
query = flattened_df.writeStream.outputMode("append")
.foreachBatch(write_to_influx)
.start()

query.awaitTermination()

Save the python file, and in new terminal run the spark job from the below command.

spark-submit --packages org.apache.spark:spark-sql-kafka- 0 - 10_2.12:3.5.1 spark.py

Here while sending the data, we look for numoutputRows= -1 which means that the data was successfully sent to
InfluxDB

Task 3: Visualize with Grafana

Setup Grafana:
Install and run Grafana by running below 2 commands in terminal

brew install grafana
brew services start grafana

Once Grafana is installed and running, open your web browser and navigate to http://localhost:3000/. The default port
for Grafana is 3000.

The default login credentials are Username: admin and Password: admin. Upon first login, you will be prompted to
change the password.

After logging in, you can configure data sources (like InfluxDB, MySQL, PostgreSQL, etc.) that Grafana will use to
pull data for visualization. Go to Configuration > Data Sources > Add data source and select the type of source you
want to add. Fill in the required connection details and settings specific to that source, as the below images showcase.

Visulization:
Once your data sources are configured, you can start creating dashboards to visualize your data.
Go to + Create > Dashboard and begin adding panels.

We can utilize flux query to fetch the data we need in our desired format to create and add visuals

Below is the query used:

After the query is run, select the desired visualization from the right hand side and configure it to your use case.

Once done Dashboard is ready, this dashboard updates the live score of the NBA game at every one-minute interval

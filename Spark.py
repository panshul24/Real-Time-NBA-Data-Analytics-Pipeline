from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import *
from influxdb_client import InfluxDBClient, Point, WriteOptions

# InfluxDB Client Configuration
url = "https://us-east-1-1.aws.cloud2.influxdata.com"
token = "Lwwpn1_X1cApjJuTHtE3TdxYzZHrGa2UH1PoGm3xFckwUJC4YPK6ByvRIV-J95UKoeG6ivmjtjBn6sBY1IOncg=="
org = "UIUC"
bucket = "nba"

client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=WriteOptions(batch_size=1000, flush_interval=10_000))



# Define the schema based on the nested JSON structure
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

# Initialize Spark Session
spark = SparkSession.builder.appName("NBA Scores Kafka Consumer").getOrCreate()

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "nba-scoreboard-1") \
    .load()

# Select message value and convert from bytes to string
df = df.selectExpr("CAST(value AS STRING)")

# Parse the JSON string using the defined schema
parsed_df = df.select(from_json(col("value"), schema).alias("data"))

# Explode the games array into individual game records
games_df = parsed_df.select(explode(col("data.scoreboard.games")).alias("game"))

# Select relevant fields for processing
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

# Function to write each batch of DataFrame to InfluxDB
def write_to_influx(batch_df, epoch_id):
    points = []
    for row in batch_df.collect():
        point = Point("nba_games") \
            .tag("gameId", row.gameId) \
            .tag("gameCode", row.gameCode) \
            .field("gameStatusText", row.gameStatusText) \
            .field("homeTeamName", row.homeTeamName) \
            .field("homeScore", int(row.homeScore)) \
            .field("awayTeamName", row.awayTeamName) \
            .field("awayScore", int(row.awayScore)) \
            .field("period", int(row.period)) \
            .time(row.gameTimeUTC)
        points.append(point)
    write_api.write(bucket=bucket, record=points)

# Write the stream to InfluxDB
query = flattened_df.writeStream.outputMode("append") \
    .foreachBatch(write_to_influx) \
    .start()

query.awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("MusicPulse") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("track_id",   StringType()),
    StructField("track_name", StringType()),
    StructField("artist",     StringType()),
    StructField("energy",     DoubleType()),
    StructField("tempo",      DoubleType()),
    StructField("valence",    DoubleType()),
    StructField("timestamp",  StringType()),
])

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "spotify_stream") \
    .option("startingOffsets", "earliest") \
    .load()

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
 .withColumn("event_time", to_timestamp("timestamp"))

windowed = parsed \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(window("event_time", "1 minute", "10 seconds")) \
    .agg(
        avg("tempo").alias("avg_tempo"),
        avg("energy").alias("avg_energy"),
        avg("valence").alias("avg_valence"),
    )

JDBC_URL = "jdbc:postgresql://postgres:5432/musicpulse"
JDBC_PROPS = {
    "user": "music",
    "password": "music",
    "driver": "org.postgresql.Driver"
}

def write_to_pg(batch_df, _):
    batch_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "avg_tempo", "avg_energy", "avg_valence"
    ).write.jdbc(JDBC_URL, "music_metrics", mode="append", properties=JDBC_PROPS)

windowed.writeStream \
    .foreachBatch(write_to_pg) \
    .option("checkpointLocation", "/tmp/musicpulse_checkpoint") \
    .outputMode("update") \
    .start() \
    .awaitTermination()
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

import boto3

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 spark_job.py
#s3://jonathan-wcd-midterm/landing


if __name__ == "__main__":
    spark = SparkSession.builder.appName("dota_streaming").getOrCreate()

    # read data from kafka

    df = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "dota")\
        .option("startingOffsets", "earliest")\
        .load()

    df.selectExpr("CAST(value AS STRING)")

    # test if it actually consumes from our kafka cluster

    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "dota").option("startingOffsets", "earliest").load()

    # we handle the transformations and everything first
    # everything above can be handled later (I hope)
    # cause kafka doesn't like me when I try to integrate spark with it

    df = spark.read.json(r"D:\WCD\Dota_Streaming\data\sample\match_6102206720.json", multiLine = "true")

    # flatten the dataframe into something more palatable

    flatten_df = df.withColumn("new", arrays_zip("result.players.hero_id", 
                                                "result.players.account_id",
                                                "result.players.player_slot",
                                                "result.players.item_0", 
                                                "result.players.item_1",
                                                "result.players.item_2",
                                                "result.players.item_3",
                                                "result.players.item_4",
                                                "result.players.item_5",
                                                "result.players.backpack_0",
                                                "result.players.backpack_1",
                                                "result.players.backpack_2",
                                                "result.players.item_neutral",
                                                "result.players.xp_per_min",
                                                "result.players.gold_per_min"))\
                    .withColumn("new", explode("new"))\
                    .select("result.match_id", 
                            "result.duration", 
                            "result.start_time", 
                            "result.radiant_win",
                            col("new.0").alias("hero_id"),
                            col("new.1").alias("account_id"),
                            col("new.2").alias("player_slot"),
                            col("new.3").alias("item_0_id"),
                            col("new.4").alias("item_1_id"),
                            col("new.5").alias("item_2_id"),
                            col("new.6").alias("item_3_id"),
                            col("new.7").alias("item_4_id"),
                            col("new.8").alias("item_5_id"),
                            col("new.9").alias("backpack_0_id"),
                            col("new.10").alias("backpack_1_id"),
                            col("new.11").alias("backpack_2_id"),
                            col("new.12").alias("item_neutral_id"),
                            col("new.13").alias("xp_per_min"),
                            col("new.14").alias("gold_per_min"))

    # combine with heroes and items json to make a human readable set of heroes/items

    # flatten and join heroes_df with flatten_df

    heroes_df = spark.read.json(r"D:\WCD\Dota_Streaming\data\heroes.json", multiLine = "true")
    heroes_df = heroes_df.select("result.heroes.id", "result.heroes.name")
    heroes_df = heroes_df.withColumn("new", arrays_zip("id", "name"))\
                            .withColumn("new", explode("new"))\
                            .select(col("new.id").alias("id"),
                                    col("new.name").alias("hero"))
    heroes_df = heroes_df.withColumn("hero", regexp_replace("hero", "npc_dota_hero_", ""))

    flatten_df = flatten_df.join(heroes_df, flatten_df.hero_id == heroes_df.id, "left").select(flatten_df["*"], heroes_df["hero"]).drop("hero_id")

    # flatten and join items_df with flatten_df

    items_df = spark.read.json(r"D:\WCD\Dota_Streaming\data\items.json", multiLine = "true")
    items_df = items_df.select("result.items.id", "result.items.name")
    items_df = items_df.withColumn("new", arrays_zip("id", "name"))\
                        .withColumn("new", explode("new"))\
                        .select(col("new.id").alias("id"),
                                col("new.name").alias("name"))
    items_df = items_df.withColumn("name", regexp_replace("name", "item_", ""))

    # multiple joins to cover all the items, backpacks and neutral item

    flatten_df = flatten_df.join(items_df, flatten_df.item_0_id == items_df.id, "left").select(flatten_df["*"], items_df.name.alias("item_0")).drop("item_0_id")
    flatten_df = flatten_df.join(items_df, flatten_df.item_1_id == items_df.id, "left").select(flatten_df["*"], items_df.name.alias("item_1")).drop("item_1_id")
    flatten_df = flatten_df.join(items_df, flatten_df.item_2_id == items_df.id, "left").select(flatten_df["*"], items_df.name.alias("item_2")).drop("item_2_id")
    flatten_df = flatten_df.join(items_df, flatten_df.item_3_id == items_df.id, "left").select(flatten_df["*"], items_df.name.alias("item_3")).drop("item_3_id")
    flatten_df = flatten_df.join(items_df, flatten_df.item_4_id == items_df.id, "left").select(flatten_df["*"], items_df.name.alias("item_4")).drop("item_4_id")
    flatten_df = flatten_df.join(items_df, flatten_df.item_5_id == items_df.id, "left").select(flatten_df["*"], items_df.name.alias("item_5")).drop("item_5_id")
    flatten_df = flatten_df.join(items_df, flatten_df.backpack_0_id == items_df.id, "left").select(flatten_df["*"], items_df.name.alias("backpack_0")).drop("backpack_0_id")
    flatten_df = flatten_df.join(items_df, flatten_df.backpack_1_id == items_df.id, "left").select(flatten_df["*"], items_df.name.alias("backpack_1")).drop("backpack_1_id")
    flatten_df = flatten_df.join(items_df, flatten_df.backpack_2_id == items_df.id, "left").select(flatten_df["*"], items_df.name.alias("backpack_2")).drop("backpack_2_id")
    flatten_df = flatten_df.join(items_df, flatten_df.item_neutral_id == items_df.id, "left").select(flatten_df["*"], items_df.name.alias("item_neutral")).drop("item_neutral_id")

    # convert start_time to year, month, day columns

    flatten_df = flatten_df.withColumn("year", year(from_unixtime("start_time")))\
                            .withColumn("month", month(from_unixtime("start_time")))\
                            .withColumn("day", dayofmonth(from_unixtime("start_time")))\
                            .drop("start_time")

    # convert radiant_win into win column

    flatten_df = flatten_df.withColumn("win", when((flatten_df.player_slot > 10) & (flatten_df.radiant_win == True), False)\
                                        .when((flatten_df.player_slot < 10) & (flatten_df.radiant_win == True), True)\
                                        .when((flatten_df.player_slot > 10) & (flatten_df.radiant_win != True), True)\
                                        .when((flatten_df.player_slot < 10) & (flatten_df.radiant_win != True), False))\
                                        .drop("radiant_win")

    # combine items into an array

    final_df = flatten_df.withColumn("items", array("item_0", "item_1", "item_2", "item_3", "item_4", "item_5", "backpack_0", "backpack_1", "backpack_2", "item_neutral"))\
                                    .select("match_id", "account_id", "duration", "year", "month", "day", "hero", "items", "xp_per_min", "gold_per_min", "win")

    # final step
    # write our final_df to our cassandra table


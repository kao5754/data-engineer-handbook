from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import os

def broadcast_join_maps(spark): 
    # Create DataFrames
    matches_df = spark.read.csv("../../data/matches.csv", header=True)
    maps_df = spark.read.csv("../../data/maps.csv", header=True)

    # Broadcast Join
    matches_maps_df = matches_df.join(broadcast(maps_df), "mapid")

    return matches_maps_df

def broadcast_join_medals(spark): 
    # Create DataFrames
    medals_df = spark.read.csv("../../data/medals.csv", header=True, inferSchema=True)
    medal_matches_players_df = spark.read.csv("../../data/medals_matches_players.csv", header=True, inferSchema=True)

    # Broadcast Join
    medals_players_df = medal_matches_players_df.join(broadcast(medals_df), "medal_id")
    medals_players_df.show(5)

    return medals_players_df

def bucket_join_matches(spark): 
    num_buckets = 16

    # Create DataFrames
    matches_df = spark.read.csv("../../data/matches.csv", header=True)

    #Repartition and Sort within partitions
    spark.sql("DROP TABLE IF EXISTS matches_bucketed")
    matches_df = matches_df.repartition(num_buckets, "match_id").sortWithinPartitions("match_id")
    matches_df.write.format("parquet").bucketBy(num_buckets, "match_id").mode("overwrite").saveAsTable("matches_bucketed")

    # Read Bucketed Table
    matches_bucketed_df = spark.table("matches_bucketed")
    return matches_bucketed_df

def bucket_join_matches_v1(spark): 
    num_buckets = 16

    # Create DataFrames
    matches_df = spark.read.csv("../../data/matches.csv", header=True)

    #Repartition and Sort within partitions
    spark.sql("DROP TABLE IF EXISTS matches_bucketed_v2")
    matches_df = matches_df.repartition(num_buckets, "match_id").sortWithinPartitions(["playlist_id", "map_id"])
    matches_df.write.format("parquet").bucketBy(num_buckets, "match_id").mode("overwrite").saveAsTable("matches_bucketed_v1")

    # Read Bucketed Table
    matches_bucketed_df = spark.table("matches_bucketed_v1")
    return matches_bucketed_df

def bucket_join_matches_v2(spark): 
    num_buckets = 16

    # Create DataFrames
    matches_df = spark.read.csv("../../data/matches.csv", header=True)

    #Repartition and Sort within partitions
    spark.sql("DROP TABLE IF EXISTS matches_bucketed_v2")
    matches_df = matches_df.repartition(num_buckets, "match_id").sortWithinPartitions("map_id")
    matches_df.write.format("parquet").bucketBy(num_buckets, "match_id").mode("overwrite").saveAsTable("matches_bucketed_v2")

    # Read Bucketed Table
    matches_bucketed_df = spark.table("matches_bucketed_v2")
    return matches_bucketed_df


def bucket_join_matches_v3(spark): 
    num_buckets = 16

    # Create DataFrames
    matches_df = spark.read.csv("../../data/matches.csv", header=True)

    #Repartition and Sort within partitions
    spark.sql("DROP TABLE IF EXISTS matches_bucketed_v3")
    matches_df = matches_df.repartition(num_buckets, "match_id").sortWithinPartitions("playlist_id")
    matches_df.write.format("parquet").bucketBy(num_buckets, "match_id").mode("overwrite").saveAsTable("matches_bucketed_v3")

    # Read Bucketed Table
    matches_bucketed_df = spark.table("matches_bucketed_v3")
    return matches_bucketed_df

def bucket_join_match_details(spark): 
    num_buckets = 16

    # Create DataFrames
    match_details_df = spark.read.csv("../../data/match_details.csv", header=True)

    #Repartition and Sort within partitions
    spark.sql("DROP TABLE IF EXISTS match_details_bucketed2")
    match_details_df = match_details_df.repartition(num_buckets, "match_id").sortWithinPartitions("match_id")
    match_details_df.write.format("parquet").bucketBy(num_buckets, "match_id").mode("overwrite").saveAsTable("match_details_bucketed2")

    # Read Bucketed Table
    match_details_bucketed_df = spark.table("match_details_bucketed2")
    match_details_bucketed_df.show()

def bucket_join_medals_matches_players(spark): 
    num_buckets = 16

    # Create DataFrames
    medals_matches_players_df = spark.read.csv("../../data/medals_matches_players.csv", header=True)

    #Repartition and Sort within partitions
    spark.sql("DROP TABLE IF EXISTS medals_matches_players")
    medals_matches_players_df = medals_matches_players_df.repartition(num_buckets, "match_id").sortWithinPartitions("match_id")
    medals_matches_players_df.write.format("parquet").bucketBy(num_buckets, "match_id").mode("overwrite").saveAsTable("medals_matches_players")

    # Read Bucketed Table
    match_details_bucketed_df = spark.table("medals_matches_players")
    match_details_bucketed_df.show()

def bucket_join_everything(spark, matches_df, match_details_df, medals_matches_players_df): 
    # Bucket All Dataframes 
    bucketd_df = matches_df.join(match_details_df, "match_id").join(medals_matches_players_df, ["match_id", "player_gamertag"])


def get_aggreagte_stats(spark, df): 
    # Get the average kills per game for each player
    df.groupBy("match_id", "player_gamertag").mean("player_total_kills").alias("average_kills_per_game").show()

    # Get the most common playlist
    most_common_playlist = df.groupBy("playlist").count().orderBy("count", ascending=False).take(1)
    print(f"The playlist that gets played the most is: {most_common_playlist}")
          
    # Get the most common map
    most_common_map = df.groupBy("mapid").count().orderBy("count", ascending=False).take(1)
    print(f"The map that gets played the most is: {most_common_map}")

    # Get the most common map for killing spree classification
    medals_df = spark.read.csv("../../data/medals.csv", header=True)
    df = df.join(broadcast(medals_df), "medal_id")
    killing_sprees = df.filter(df.classification == "KillingSpree").groupBy("mapid").count().orderBy("count", ascending=False).take(1)
    print(f"The most common map for KillingSpree is {killing_sprees}")

def compare_file_sizes(spark):
    # Compare the file sizes of 3 bucketed dataframes
    tables = ["matches_bucketed_v1", "matches_bucketed_v2", "matches_bucketed_v3"]

    for table in tables: 
        file_path = "spark-warehouse/" + table
        file_sizes = []
        if os.path.exists(file_path): 
            for file in os.listdir(file_path): 
                file_sizes.append(os.path.getsize(file_path + "/" + file))
            print(f"Table: {table}, file sizes: {file_sizes}")


def main(): 
    # Create SparkSession
    spark = SparkSession.builder \
        .config("spark.sql.autoBroadcastJoinThreshold", -1) \
        .appName("MatchStats") \
        .getOrCreate()

    # Broadcast join maps and matches dataframes
    broadcast_map = broadcast_join_maps(spark)
    
    # Broadcast join medals and medal_matches_players DataFrames
    broadcast_medals = broadcast_join_medals(spark)
    broadcast_medals.show()

    # Bucket Matches
    match_bucket = bucket_join_matches(spark)

    # Bucket Match Details
    match_details_bucket = bucket_join_match_details(spark)

    # Bucket Medals Matches Players
    medals_matches_players_bucket = bucket_join_medals_matches_players(spark)

    # Join Buckets: 
    everything_bucket = bucket_join_everything(spark, match_bucket, match_details_bucket, medals_matches_players_bucket)

    # Run Stats
    get_aggreagte_stats(spark, everything_bucket)

    # Compare Various sort partitions
    compare_file_sizes(spark)


if __name__ == "__main__": 
    main()
# Initialisation and Creation of source DFs
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, split, lit, avg, col

spark = SparkSession.builder.appName("Assignment").getOrCreate()

spark

match_details = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")

matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv").withColumnRenamed('mapid', 'map_id')

maps = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv").withColumnRenamed('mapid', 'map_id')

medals = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")

medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")

# Disable Broadcast Join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Create bootcamp database
spark.sql("""CREATE DATABASE IF NOT EXISTS bootcamp""")

# Broadcast Join of Map with Matches on map_id
matches_df = matches.join(broadcast(maps),"map_id", "inner")

# Broadcast Join of Medals with Medals_Matches_Players on medal_id
medals_matches_players_df = medals_matches_players.join(broadcast(medals), "medal_id", "inner")

# Matches
spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")

matches_bucketedDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
    match_id STRING,
    map_id STRING,
    name STRING,
    description STRING,
    is_team_game BOOLEAN,
    playlist_id STRING,
    completion_date DATE,
    match_duration STRING
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """

spark.sql(matches_bucketedDDL)

matches_df.select(col("match_id"), col("map_id"), col("name"), col("description"), col("is_team_game"), col("playlist_id"), col("completion_date"), col("match_duration")) \
        .write \
        .mode("append") \
        .bucketBy(16, "match_id") \
        .saveAsTable("bootcamp.matches_bucketed")


# Match Details

spark.sql("""DROP TABLE IF EXISTS bootcamp.match_details_bucketed""")

match_details_bucketedDDL = """
 CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
     match_id STRING,
     player_gamertag STRING,
     player_total_kills INTEGER,
     player_total_deaths INTEGER
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
"""

spark.sql(match_details_bucketedDDL)

match_details.select(col("match_id"), col("player_gamertag"), col("player_total_kills"), col("player_total_deaths")) \
     .write.mode("append") \
     .bucketBy(16, "match_id").saveAsTable("bootcamp.match_details_bucketed")

# Medals Matches Players

spark.sql("""DROP TABLE IF EXISTS bootcamp.medals_matches_players_bucketed""")

medals_matches_players_bucketedDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_bucketed (
    match_id STRING,
    player_gamertag STRING,
    medal_id STRING,
    classification STRING,
    description STRING,
    difficulty STRING,
    count INTEGER
)
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
"""

spark.sql(medals_matches_players_bucketedDDL)

medals_matches_players_df.select(col("match_id"),col("player_gamertag"),col("medal_id"), col("classification"), col("description"), col("difficulty"), col("count")) \
                      .write.mode("append") \
                      .bucketBy(16, "match_id").saveAsTable("bootcamp.medals_matches_players_bucketed")

# Join medals_matches_players, match_details, matches on bucketed 16

md_df = spark.table("bootcamp.match_details_bucketed")
m_df = spark.table("bootcamp.matches_bucketed")
mmp_df = spark.table("bootcamp.medals_matches_players_bucketed")

result_df = (
    md_df.alias("mdb")
    .join(
        m_df.alias("md"),
        on=md_df.match_id == m_df.match_id,
    )
    .join(
        mmp_df.alias("mmp"),
        on=(
            (md_df.match_id == mmp_df.match_id)
            & (md_df.player_gamertag == mmp_df.player_gamertag)
        ),
    )
    # Select the required columns
    .select(
        "mdb.match_id",
        "mdb.player_gamertag",
        "mdb.player_total_kills",
        "mdb.player_total_deaths",
        "md.playlist_id",
        "md.map_id",
        "md.name",
        "md.completion_date",
        "mmp.medal_id",
        "mmp.classification",
        "mmp.count",
    )
)

result_df.show()

# Create aggregated table for reference

spark.sql("""DROP TABLE IF EXISTS bootcamp.aggregated_table""")

agg_table_DDL = """ 
CREATE TABLE IF NOT EXISTS bootcamp.aggregated_table (
    match_id STRING, 
    player_gamertag STRING,
    player_total_kills INTEGER,
    player_total_deaths INTEGER,
    playlist_id STRING,
    map_id STRING,
    name STRING,
    completion_date DATE,
    medal_id STRING,
    classification STRING,
    count INTEGER
)
"""

spark.sql(agg_table_DDL)

result_df.select("*") \
         .write.mode("append") \
         .saveAsTable("bootcamp.aggregated_table")


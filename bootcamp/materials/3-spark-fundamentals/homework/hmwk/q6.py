#DDL for Unsorted Agg Table
spark.sql("""DROP TABLE IF EXISTS bootcamp.unsorted_agg_table""")

unsorted_agg_table = """ 
CREATE TABLE IF NOT EXISTS bootcamp.unsorted_agg_table (
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

spark.sql(unsorted_agg_table)

result_df.select("*") \
         .write.mode("append") \
         .saveAsTable("bootcamp.unsorted_agg_table")

#DDL for Sorted Agg Table
spark.sql("""DROP TABLE IF EXISTS bootcamp.sorted_agg_table""")

sorted_agg_table = """ 
CREATE TABLE IF NOT EXISTS bootcamp.sorted_agg_table (
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

spark.sql(sorted_agg_table)

result_df.select("*") \
         .write.mode("append") \
         .saveAsTable("bootcamp.sorted_agg_table")

# Different Partition
start_df = result_df.repartition(4, col("playlist_id"))

first_sort_df = start_df.sortWithinPartitions(col("playlist_id"), col("map_id"))

start_df.write.mode("overwrite").saveAsTable("bootcamp.unsorted_agg_table")
first_sort_df.write.mode("overwrite").saveAsTable("bootcamp.sorted_agg_table")

spark.sql("""SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
FROM bootcamp.sorted_agg_table.files
UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted' 
FROM bootcamp.unsorted_agg_table.files
""").show()
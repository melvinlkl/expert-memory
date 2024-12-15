from pyspark.sql import SparkSession

query = """
WITH dedup AS (SELECT
    md.*,
    -- Partition by team_id ensures unique combination
    ROW_NUMBER() OVER(PARTITION BY md.team_id ORDER BY game_id) AS row_num
FROM match_details md)

SELECT 
game_id,
team_id, 
player_id,
pts
FROM dedup
WHERE row_num = 1
"""

def do_dedup_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("match_details")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
            .master("local") \
            .appName("Spark Dedup") \
            .getOrCreate()
    output_df = do_dedup_transformation(spark, spark.table("match_details"))
    output_df.write.mode("overwrite").insertInto("players_dedup")

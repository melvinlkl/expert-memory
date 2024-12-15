from pyspark.sql import SparkSession


query = """ 
-- To convert days active into numbers with powers of 2
WITH bits AS (
         SELECT user_id,
                CAST(SUM(CASE
                        WHEN is_active THEN POW(2, 32 - days_since)
                        ELSE 0 END) AS LONG) AS datelist_int,
                DATE('2023-01-02') as cur_date
         FROM datelist
         GROUP BY user_id
     )
     SELECT * FROM bits 
"""


def do_datelist_int_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("datelist")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("datelist_int") \
      .getOrCreate()
    output_df = do_datelist_int_transformation(spark, spark.table("datelist"))
    output_df.write.mode("overwrite").insertInto("datelist_int")
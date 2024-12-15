from chispa.dataframe_comparer import *
from ..jobs.dedup_job import do_dedup_transformation
from collections import namedtuple
InputData = namedtuple("InputData", "game_id team_id player_id pts")
OutputData = namedtuple("OutputData", "game_id team_id player_id pts")

def test_dedup(spark):
    source_data = [
        InputData(1, 'GSW', 101, 10),
        InputData(2, 'GSW', 102, 5),
        InputData(1, 'LAL', 111, 5),
        InputData(2, 'LAL', 112, 5)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_dedup_transformation(spark, source_df)
    expected_data = [
        OutputData(1, 'GSW', 101, 10),
        OutputData(1, 'LAL', 111, 5)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
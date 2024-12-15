from chispa.dataframe_comparer import *
import datetime
from ..jobs.datelist_job import do_datelist_int_transformation
from collections import namedtuple
DatelistInput = namedtuple("DatelistInput", "user_id is_active days_since cur_date")
DatelistOutput = namedtuple("DatelistOutput", "user_id datelist_int cur_date")


def test_datelist_transformation(spark):
    source_data = [
        DatelistInput(1, True, 0, '2023-01-02'),
        DatelistInput(2, False, 1, '2023-01-02')
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_datelist_int_transformation(spark, source_df)
    expected_data = [
        DatelistOutput(1, 4294967296, datetime.date(2023, 1, 2)),
        DatelistOutput(2, 0, datetime.date(2023, 1, 2))
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)
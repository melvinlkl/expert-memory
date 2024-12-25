#Which map gets played the most?
most_played_map = result_df.groupBy(["map_id", "name"]) \
                           .count() \
                           .orderBy("count",ascending=False) \
                           .show(n=1)
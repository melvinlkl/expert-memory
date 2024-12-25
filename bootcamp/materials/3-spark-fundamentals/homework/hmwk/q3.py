#Which playlist gets played the most?
most_played_playlist = result_df.groupBy(["playlist_id"]) \
                                .count() \
                                .orderBy("count",ascending=False) \
                                .show(n=1)
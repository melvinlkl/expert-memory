#Which player averages the most kills per game?
most_kills_per_game = result_df.groupBy(["player_gamertag", "match_id"]) \
                               .agg({"player_total_kills": "avg"}) \
                               .orderBy("avg(player_total_kills)",ascending=False) \
                               .show(n=1)
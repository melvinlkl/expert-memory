#Which map do players get the most killing spree medals on?
most_killing_spree = result_df.groupBy(["map_id", "name", "classification"]) \
                              .agg({"count": "sum"}) \
                              .filter(result_df.classification == "KillingSpree") \
                              .orderBy("sum(count)", ascending=False) \
                              .show(n=1)
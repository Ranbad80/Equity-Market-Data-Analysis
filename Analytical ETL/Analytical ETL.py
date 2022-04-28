from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import datetime

spark = SparkSession.builder.master('local').appName('app').getOrCreate()
# sc = SparkContext()
spark.conf.set("fs.azure.account.key.blobstorageran.blob.core.windows.net",
               "OOSzVsZ6TshIgo/4ijZNPkQP70M7G4jglGkOvtRibMnEbyM4rS79ikwAC/B/JyRwOwW38TLzQoZF+AStkcuyoQ=="
               )

# Read Parquet Files From Azure Blob Storage Partition
df = spark.read.parquet("wasbs: // mycontainer @ blobstorageran.blob.core.windows.net/trade/date={}".format("2020-07-29"))

#Create Trade Staging Table
#Use Spark To Read The Trade Table With Date Partition “2020-07-29”
df=spark.sql("select symbol, event_tm, event_seq_nb, trade_pr +\
              from trades  \
              where trade_dt='2020-07-29'")

#Create A Spark Temporary View
df.createOrReplaceTempView("tmp_trade_moving_avg")


mov_avg_df = spark.sql("""
    select symbol, exchange, event_tm, event_seq_nb,trade_pr,AVG(trade_pr) 
    OVER(PARTITION BY symbol
         ORDER BY CAST(event_tm AS timestamp)
         RANGE BETWEEN INTERVAL '30' MINUTES PRECEDING
         AND CURRENT ROW) AS mov_avg_pr 
    from tmp_trade_moving_avg""")

#Save The Temporary View Into Hive Table For Staging
mov_avg_df.write.saveAsTable("temp_trade_moving_avg")
mov_avg_df.show()


#Create Staging Table For The Prior Day’s Last Trade
#Get The Previous Date Value
date = datetime.datetime.strptime('2020-08-04', '%Y-%m-%d')
prev_date_str = date - datetime.timedelta(days=1)  #date_sub("date", 1)
print(prev_date_str)


#Use Spark To Read The Trade Table With Date Partition “2020-07-28”
prev_date_str = "2020-07-28"
df = spark.sql("select symbol, event_tm, event_seq_nb, trade_pr from trades \
               where trade_dt='{}'".format(prev_date_str))

# Create Spark Temporary View
df.createOrReplaceTempView("tmp_last_trade")

#Calculate Last Trade Price Using The Spark Temp View
last_pr_df = spark.sql("""select symbol, exchange, last_pr from (select
symbol, exchange, event_tm, event_seq_nb, trade_pr,LAST_VALUE(mov_avg_pr) OVER(PARTITION BY symbol
         ORDER BY CAST(event_tm AS timestamp) AS last_pr
FROM tmp_trade_moving_avg) a
""")

#Save The Temporary View Into Hive Table For Staging
last_pr_df.write.saveAsTable("temp_last_trade")


#Populate The Latest Trade and Latest Moving Average Trade Price To The Quote Records
#Join With Table temp_trade_moving_avg
#Define A Common Schema Holding “quotes” and “temp_trade_moving_avg” Records
#Create Spark Temp View To Union Both Tables
quote_union = spark.sql("""
                    select trade_dt,
                           "Q" as rec_type,
                           symbol,
                           event_tm,
                           event_seq_nb,
                           exchange,
                           bid_pr,
                           bid_size,
                           ask_pr,
                           ask_size,
                           null as trade_pr,
                           null as mov_avg_pr
                    from quotes
                    union all
                    select trade_dt,
                           "T" as rec_type,
                           symbol,
                           event_tm,
                           null as event_seq_nb,
                           exchange,
                           null as bid_pr,
                           null as bid_size,
                           null as ask_pr,
                           null as ask_size,
                           trade_pr,
                           mov_avg_pr
                    from temp_trade_moving_avg""")

quote_union.show(10)
quote_union.createOrReplaceTempView("quote_union")

#Populate The Latest trade_pr and mov_avg_pr
quote_union_update = spark.sql("""
                          select *,
                                 LAST_VALUE(trade_pr,true) OVER(PARTITION BY symbole,exchange
                                 ORDER BY CAST(event_tm AS timestamp) AS last_trade_pr,
                                 LAST_VALUE(mov_avg_pr,true) OVER(PARTITION BY symbole,exchange
                                 ORDER BY CAST(event_tm AS timestamp) AS last_mov_avg_pr,
                           from quote_union
   """)
quote_union_update.createOrReplaceTempView("quote_union_update")


#Filter For Quote Records
quote_update = spark.sql("""
   select trade_dt, symbol, event_tm, event_seq_nb, exchange,
   bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr
   from quote_union_update
   where rec_type = 'Q'
   """)
quote_update.createOrReplaceTempView("quote_update")

#Join With Table temp_last_trade To Get The Prior Day Close Price
quote_final = spark.sql("""
   select trade_dt, symbol, event_tm, event_seq_nb, exchange,
   bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr,
   bid_pr - close_pr as bid_pr_mv, ask_pr - close_pr as ask_pr_mv
   from (SELECT /*+ BROADCAST(temp_last_trade) */ 
          qu.trade_dt,
          qu.symbol, 
          qu.event_tm, 
          qu.event_seq_nb, 
          qu.exchange,
          qu.bid_pr, 
          qu.bid_size, 
          qu.ask_pr, 
          qu.ask_size, 
          qu.last_trade_pr, 
          qu.last_mov_avg_pr,
          tl.close_pr 
         from quote_update qu
         left outer join  temp_last_trade tl
         on qu.symbol=tl.symbol and qu.exchange=tl.exchange
        )a
""")
quote_final.show()

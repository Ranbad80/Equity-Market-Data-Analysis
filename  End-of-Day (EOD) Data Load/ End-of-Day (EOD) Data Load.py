from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, DecimalType

spark = SparkSession.builder.master('local').appName('app').getOrCreate()
# sc = SparkContext()
spark.conf.set("fs.azure.account.key.blobstorageran.blob.core.windows.net",
               "OOSzVsZ6TshIgo/4ijZNPkQP70M7G4jglGkOvtRibMnEbyM4rS79ikwAC/B/JyRwOwW38TLzQoZF+AStkcuyoQ=="
               )

# Read Trade and quote Partition Dataset From It’s Temporary Location
trade_common = spark.read.parquet("output_dir/partition=T")
quote_common = spark.read.parquet("output_dir/partition=Q")

# Select The Necessary Columns For Trade Records
trade = trade_common.select("trade_dt", "symbol", "exchange", "event_tm",
                            "event_seq_nb", "arrival_tm", "trade_pr")

# Select The Necessary Columns For Quote Records
quote = quote_common.select("trade_dt", "symbol", "exchange", "event_tm",
                            "event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")


#Apply Data Correction
def applyLatest(df):
    df1 = df.groupBy("trade_dt", "symbol", "exchange",
                     "event_tm", "event_seq_nb").agg(F.max("arrival_tm").collect_set()[0].alias("latest_arrival_tm"))
    return df1


# Use the method
trade_corrected = applyLatest(trade)
quote_corrected = applyLatest(quote)

cloud_storage_path = "wasbs: // mycontainer @ blobstorageran.blob.core.windows.net"
trade_date = trade_corrected.select("trade_dt")
trade.write.parquet("cloud_storage_path/trade/trade_dt={}".format(trade_date))


quote_date = quote_corrected.select("trade_dt")
trade.write.parquet("cloud_storage_path/quote/trade_dt={}".format(quote_date))

import datetime
import psycopg2
import mysql.connector


class Tracker(object):
   """
   job_id, status, updated_time
   """

   def __init__(self, jobname, dbconfig):
       self.jobname = jobname
       self.dbconfig = dbconfig

   def assign_job_id(self):
       # [Construct the job ID and assign to the return variable]
       job_id = self.jobname + "_" + datetime.date.today()
       return job_id

   def update_job_status(self, status):
       job_id = self.assign_job_id()
       print("Job ID Assigned: {}".format(job_id))
       update_time = datetime.datetime.now()
       table_name = self.dbconfig.get("postgres", "job_tracker_table_name")
       connection = self.get_db_connection()

       try:
           # [Execute the SQL statement to insert to job status table]
           mycursor = connection.cursor()
           sql_insert = "INSERT INTO job_tracker_table_name (job_id, status, updated_time) VALUES (%s, %s,%s)"
           val = ('" + job_id + "', '" + status + "',
                  '" + str(update_time) + "')
           mycursor.execute(sql_insert)
           mycursor.close()
       except (Exception, psycopg2.Error) as error:
           print("error executing db statement for job tracker.")
           return logging.error("Error executing db statement for job tracker.", error)

   def get_job_status(self, job_id):
        # connect db and send sql query
        sql_query = "select * from job_tracker_table_name where job_id=job_id "
        table_name = self.dbconfig.get('postgres', 'job_tracker_table_name')
        connection = self.get_db_connection()
        mycursor = connection.cursor()
        try:
             record = mycursor.execute(sql_query)
             return record
        except (Exception, psycopg2.Error) as error:
             print("error executing db statement for job tracker.")
             return error

   def get_db_connection(self):
        connection = None
        try:
            connection = mysql.connector.connect(host='localhost',
                                         database='db',
                                         user='root',
                                         password='#####')
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
            logging.error("Error while connecting to database for job tracker", error)
        return connection

   #run (multiple instances of the same job running simultaneously)
   def data_ingestion(self):
       def parse_csv(line: str):
           record_type_pos = 2
           record = line.split(",")
           try:
               if record[record_type_pos] == "T":
                   trade_dt = datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f').date()
                   rec_type = record[2]
                   symbol = record[3]
                   event_tm = datetime.strptime(record[4], "%Y-%m-%d %H:%M:S.%f")
                   event_seq_nb = int(record[5])
                   arrival_tm = datetime.now()
                   exchange = record[6]
                   trade_pr = float(record[7])
                   Trade_Size = int(record[8])
                   bid_pr = None
                   bid_size = None
                   ask_pr = None
                   ask_size = None
                   partition = "T"

                   event = (partition, trade_dt, rec_type, symbol, exchange,
                            event_tm, event_seq_nb, arrival_tm, trade_pr, Trade_Size, bid_pr, bid_size, ask_pr,
                            ask_size, "")
                   return event

               elif record[record_type_pos] == "Q":
                   trade_dt = datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f').date()
                   rec_type = record[2]
                   symbol = record[3]
                   event_tm = datetime.strptime(record[4], "%Y-%m-%d %H:%M:S.%f")
                   event_seq_nb = int(record[5])
                   arrival_tm = datetime.now()
                   exchange = record[6]
                   trade_pr = None
                   Trade_Size = None
                   bid_pr = float(record[7])
                   bid_size = int(record[8])
                   ask_pr = float(record[9])
                   ask_size = int(record[10])
                   partition = "Q"

                   event = (partition, trade_dt, rec_type, symbol, exchange,
                            event_tm, event_seq_nb, arrival_tm, trade_pr, Trade_Size, bid_pr, bid_size, ask_pr,
                            ask_size, "")
                   return event

           except Exception as e:
               trade_dt = None
               rec_type = None
               symbol = None
               event_tm = None
               event_seq_nb = None
               arrival_tm = datetime.now()
               exchange = None
               trade_pr = None
               Trade_Size = None
               bid_pr = None
               bid_size = None
               ask_pr = None
               ask_size = None
               partition = "B"

               event = (partition, trade_dt, rec_type, symbol, exchange,
                        event_tm, event_seq_nb, arrival_tm, trade_pr, Trade_Size, bid_pr, bid_size, ask_pr, ask_size,
                        line)
               return event

       def parse_json(line: str):
           record = line.split(",")
           record_type = record['event_type']

           try:
               if record_type == "T":
                   if record.key():
                       trade_dt = datetime.strptime(record["trade_dt"], '%Y-%m-%d %H:%M:%S.%f').date()
                       rec_type = record["event_type"]
                       symbol = record["symbol"]
                       event_tm = datetime.strptime(record["event_tm"], "%Y-%m-%d %H:%M:S.%f")
                       event_seq_nb = int(record["event_seq_nb"])
                       arrival_tm = datetime.now()
                       exchange = record["exchange"]
                       trade_pr = float(record["price"])
                       Trade_Size = int(record["size"])
                       bid_pr = 0.0
                       bid_size = 0
                       ask_pr = 0.0
                       ask_size = 0
                       partition = "T"

                       event = (partition, trade_dt, rec_type, symbol, exchange,
                                event_tm, event_seq_nb, arrival_tm, trade_pr, Trade_Size, bid_pr, bid_size, ask_pr,
                                ask_size, "")
                   else:
                       partition = "B"
                       event = (partition, None, None, None, None, None, None,
                                None, None, None, None, None, None, line)

                   return event

               elif record_type == "Q":
                   if record.key():
                       trade_dt = datetime.strptime(
                           record["trade_dt"], '%Y-%m-%d %H:%M:%S.%f').date()
                       rec_type = record["event_type"]
                       symbol = record["symbol"]
                       event_tm = datetime.strptime(
                           record["event_tm"], "%Y-%m-%d %H:%M:S.%f")
                       event_seq_nb = int(record["event_seq_nb"])
                       arrival_tm = datetime.now()
                       exchange = record["exchange"]
                       trade_pr = None
                       Trade_Size = None
                       bid_pr = float(record["bid_pr"])
                       bid_size = int(record["bid_size"])
                       ask_pr = float(record["ask_pr"])
                       ask_size = int(record["ask_size"])
                       partition = "Q"

                       event = (partition, trade_dt, rec_type, symbol, exchange,
                                event_tm, event_seq_nb, arrival_tm, trade_pr, Trade_Size, bid_pr, bid_size, ask_pr,
                                ask_size, "")
                   else:
                       partition = "B"
                       event = (partition, None, None, None, None, None, None,
                                None, None, None, None, None, None, None, line)
                   return event

           except Exception as e:
               trade_dt = None
               rec_type = None
               symbol = None
               event_tm = None
               event_seq_nb = None
               arrival_tm = datetime.now()
               exchange = None
               trade_pr = None
               Trade_Size = None
               bid_pr = None
               bid_size = None
               ask_pr = None
               ask_size = None
               partition = "B"

               event = (partition, trade_dt, rec_type, symbol, exchange,
                        event_tm, event_seq_nb, arrival_tm, trade_pr, Trade_Size, bid_pr, bid_size, ask_pr, ask_size,
                        line)
               return event

       record_schema = StructType([
           StructField("partition", StringType()),
           StructField("trade_dt", DateType()),
           StructField("symbol", StringType()),
           StructField("exchange", StringType()),
           StructField("event_tm", DateType()),
           StructField("event_seq_nb", IntegerType()),
           StructField("arrival_tm", DateType()),
           StructField("trade_pr", FloatType()),
           StructField("bid_pr", FloatType()),
           StructField("bid_size", IntegerType()),
           StructField("ask_pr", FloatType()),
           StructField("ask_size", IntegerType())])

       spark = SparkSession.builder.master('local').appName('app').getOrCreate()
       sc = spark.sparkContext
       spark.conf.set("fs.azure.account.key.blobstorageran.blob.core.windows.net",
                      "OOSzVsZ6TshIgo/4ijZNPkQP70M7G4jglGkOvtRibMnEbyM4rS79ikwAC/B/JyRwOwW38TLzQoZF+AStkcuyoQ=="
                      )
       raw1 = sc.textFile(
           "wasbs: // mycontainer @ blobstorageran.blob.core.windows.net/csv")
       parsed1 = raw1.map(lambda line: parse_csv(line))
       data1 = spark.createDataFrame(parsed1, record_schema)

       raw2 = sc.textFile(
           "wasbs: // mycontainer @ blobstorageran.blob.core.windows.net/json")
       parsed2 = raw2.map(lambda line: parse_json(line))
       data2 = spark.createDataFrame(parsed2, record_schema)

       data = data1.union(data2)

       data.write.partitionBy("partition").mode("overwrite").parquet("output_dir")




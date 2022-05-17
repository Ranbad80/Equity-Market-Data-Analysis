def run_reporter_etl(my_config):
   trade_date = my_config.get('production', 'processing_date')
   reporter = Reporter(spark, my_config)
   tracker = Tracker('analytical_etl', my_config)
   try:
       reporter.report(spark, trade_date, eod_dir)
       tracker.update_job_status("success")
   except Exception as e:
       print(e)
       tracker.update_job_status("failed")
   return
# Call above functions using arguments created and specified below
if __name__ == "__main__":
    # Get logging info
    logger = logging.getLogger(__name__)
    # Write data to logfile
    logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s',
                        datefmt='%m%d%Y %I:%M:%S',
                        level=logging.DEBUG)
    # StreamHandler object to send logging output to streams such as sys.stdout, sys.stderr.
    sh = logging.StreamHandler()

    # Call run_data_ingestion(my_config)
    run_reporter_etl()

    # Enter logging information.
    logger.info("Daily Reporting ETL Job complete!")


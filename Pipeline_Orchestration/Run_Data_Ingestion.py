import sys
import logging
from Tracker import Tracker

def track_data_ingestion():

    # The method is used to keep track on the actions of the data ingestion.py
    tracker = Tracker("Data_Ingestion")
    job_id = tracker.assign_job_id()
    connection = tracker.get_db_connection()
    try:
        # In addition, create methods to assign job_id and get db connection.
        tracker.data_ingestion()
        tracker.update_job_status("Successful Data Ingestion.", job_id, connection)
    except Exception as e:
        print(e)
        tracker.update_job_status("Failed Data Ingestion.", job_id, connection)
    return


if __name__ == "__main__":
    # Get logging info
    logger=logging.getLogger(__name__)
    # Write data to logfile
    logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s',datefmt='%m%d%Y %I:%M:%S',
        level=logging.DEBUG)
    # StreamHandler object to send logging output to streams such as sys.stdout, sys.stderr.
    sh = logging.StreamHandler()

    # Call run_data_ingestion(my_config)
    track_data_ingestion()

    # Enter logging information.
    logger.info("Daily Data Ingestion Job complete!")




from exportJobSQL import main as job_export
from importJobSQL import main as job_import
import os
from dotenv import load_dotenv

load_dotenv()
import time
from pymongo import MongoClient
import logging

MONGO_URL = os.getenv('MONGO_URL')
# Setup basic configuration for logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)
client = MongoClient(MONGO_URL, maxIdleTimeMS=None)
logging.info("Mongo connection successful")

def getSyncJob(column_mapping_collection):
    logging.info("getSyncJob called...")
    column_mapping_data = list(column_mapping_collection.find())
    return column_mapping_data

def updateState(column_mapping_collection, data, status, message):
    logging.info("updateState called...")
    logging.info("Status: " + str(status))
    logging.info("Message: " + str(message))
    key_to_check = {"_id": data["_id"]}
    result = column_mapping_collection.update_one(
        key_to_check,
        {
            "$set": {
                "status": status,
                "message": message,
                "updatedAt":(int(time.time())*1000)
            }
        })
    if result.matched_count > 0:
        logging.info("Updated the document: " + str(key_to_check))
    else:
        logging.info("No updates for the document: " + str(key_to_check))

def main(retry_attempts=0):
    try:
        db = client['gstservice']
        column_mapping_collection = db['column_mapping_schema']
        logging.info("Retry Attempts: " + str(retry_attempts))
        start_time = time.time()
        logging.info("Sync starting at: " + str(start_time))
        jobs = getSyncJob(column_mapping_collection)
        logging.info("No. of Jobs: " + str(len(jobs)))
        if jobs is not None and len(jobs) != 0:
            for i in range(len(jobs)):
                logging.info("Job: "+str(jobs[i]))
                zoho_table_name = jobs[i]["za_table_name"]
                view_id = jobs[i]["zohoViewId"]
                logging.info("============================================ Started Exporting from ZA "
                             "============================================")
                export_status, export_message = job_export(zoho_table_name, view_id, 0)
                if export_status:
                    logging.info("============================================ Started Importing into PG "
                                 "============================================")
                    import_status, import_message = job_import(jobs[i])
                    updateState(column_mapping_collection, jobs[i], import_status, import_message)
                else:
                    updateState(column_mapping_collection, jobs[i], export_status, export_message)
        else:
            logging.info("No jobs to run")
        # Record the end time
        end_time = time.time()
        # Calculate the total seconds
        total_seconds = end_time - start_time
        logging.info(f"Script executed in {total_seconds:.4f} seconds")

    except Exception as e:
        logging.info("Exception occurred in data-sync: " + str(e))
        if retry_attempts > 4:
            raise Exception(e)
        main(retry_attempts + 1)


if __name__ == '__main__':
    main()

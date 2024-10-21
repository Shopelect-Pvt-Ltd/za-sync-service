from exportJobZoho import main as job_export
from importJobMongo import main as job_import
import os
from dotenv import load_dotenv

load_dotenv()
import time
from pymongo import MongoClient
import logging
import requests
from datetime import datetime
import pytz
from sendgrid.helpers.mail import Mail
import sys

MONGO_URL = os.getenv('MONGO_URL')
SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')

# Setup basic configuration for logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)
client = MongoClient(MONGO_URL, maxIdleTimeMS=None)
logging.info("Mongo connection successful")

def getSyncJob(column_mapping_collection):
    logging.info("getSyncJob called...")
    if len(sys.argv) >= 2 and sys.argv[1] == "PENDING":
        column_mapping_data = list(column_mapping_collection.find({"status": "PENDING"}))
    else:
        column_mapping_data = list(column_mapping_collection.find({"status": "PASS"}))
    return column_mapping_data

def send_email(to_emails, template_id, dynamic_template_data):
    api_key = SENDGRID_API_KEY
    url = 'https://api.sendgrid.com/v3/mail/send'

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_key}',
    }

    for to_email in to_emails:
        message = Mail(
            from_email='alerts@finkraft.ai',
            to_emails=to_email,
        )
        message.template_id = template_id
        message.dynamic_template_data = dynamic_template_data

        try:
            # Convert the Mail object to JSON
            response = requests.post(
                url,
                headers=headers,
                json=message.get(),
                verify=False  # Disable SSL verification
            )
            logging.info(f"Email sent to {to_email} successfully! Status code: {response.status_code}")
        except Exception as e:
            logging.info(f"Error sending email to {to_email}: {e}")


def updateState(column_mapping_collection, data, status, message):
    logging.info("updateState called...")
    logging.info("Status: " + str(status))
    logging.info("Message: " + str(message))
    if status != "PASS":
        za_table_name = data["za_table_name"]
        mongo_collection_name = data["mongo_collection_name"]
        to_emails = ["komalkant@kgrp.in", "sarvesh@kgrp.in"]
        template_id = "d-1331584f7ed54169b5c36894ec9c19cc"
        ist = pytz.timezone('Asia/Kolkata')
        current_time_ist = datetime.now(ist)
        dynamic_template_data = {
            "subject": "Exception happened while syncing za-data " + str(
                current_time_ist.strftime('%d-%m-%Y %H:%M:%S')),
            "description": "Exception happened while syncing za table: " + str(za_table_name) + " to mongo collection: " + str(
                mongo_collection_name) + ". Message: " + str(message)
        }
        send_email(to_emails, template_id, dynamic_template_data)

    key_to_check = {"_id": data["_id"]}
    result = column_mapping_collection.update_one(
        key_to_check,
        {
            "$set": {
                "status": status,
                "message": message,
                "updatedAt": (int(time.time()) * 1000)
            }
        })
    if result.matched_count > 0:
        logging.info("Updated the document: " + str(key_to_check))
    else:
        logging.info("No updates for the document: " + str(key_to_check))

def main(retry_attempts=0):
    try:
        db = client['za_mongo_sync']
        column_mapping_collection = db['column_mapping_schema']
        logging.info("Retry Attempts: " + str(retry_attempts))
        start_time = time.time()
        logging.info("Sync starting at: " + str(start_time))
        jobs = getSyncJob(column_mapping_collection)
        logging.info("No. of Jobs: " + str(len(jobs)))
        if jobs is not None and len(jobs) != 0:
            for i in range(len(jobs)):
                logging.info("Job: " + str(jobs[i]))
                zoho_table_name = jobs[i]["za_table_name"]
                view_id = jobs[i]["zohoViewId"]
                logging.info("============================================ Started Exporting from ZA "
                             "============================================")
                export_status, export_message = job_export(zoho_table_name, view_id, 0)
                if export_status == "PASS":
                    logging.info("============================================ Started Importing into MONGO "
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

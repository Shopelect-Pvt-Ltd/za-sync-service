import logging
import time
import os
from dotenv import load_dotenv
load_dotenv()
from AnalyticsClient import AnalyticsClient
# Setup basic configuration for logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)

ZA_CLIENT_ID = os.getenv("ZA_CLIENT_ID")
ZA_CLIENT_SECRET = os.getenv("ZA_CLIENT_SECRET")
ZA_REFRESH_TOKEN = os.getenv("ZA_REFRESH_TOKEN")

ORGID = os.getenv("ORG_ID")
WORKSPACEID = os.getenv("WORKSPACE_ID")
JOBID = os.getenv("JOB_ID")

response_format = 'CSV'

ac = AnalyticsClient(
    client_id=ZA_CLIENT_ID,
    client_secret=ZA_CLIENT_SECRET,
    refresh_token=ZA_REFRESH_TOKEN
)

def zoho_get_sql(bulk, sql_query, file_path):
    JOBID = bulk.initiate_bulk_export_using_sql(sql_query, response_format)
    flag = False
    while True:
        response = bulk.get_export_job_details(JOBID)
        status = response
        logging.info(status)
        if status['jobStatus'] == 'JOB COMPLETED':
            logging.info('Export job completed successfully.')
            flag = True
            break
        elif status['jobStatus'] == 'ERROR OCCURRED':
            logging.info('Export job failed.')
            break
        else:
            logging.info('Export job is still in progress... '+ str(int(time.time())))
            time.sleep(60)

    result = bulk.export_bulk_data(JOBID, file_path=file_path)
    logging.info("CSV downloaded at: "+str(int(time.time())))
    return flag


def main(zoho_table_name, view_id, retry_attempts=0):
    try:
        logging.info("Retry Attempts "+str(retry_attempts))
        bulk = ac.get_bulk_instance(ORGID, WORKSPACEID)
        file_path = f"./{zoho_table_name}.csv"
        logging.info("File Path: "+str(file_path))
        # # Get job details
        sql_query = f"select * from `{zoho_table_name}` limit 10 """
        logging.info("Import Query: "+str(sql_query))
        state = zoho_get_sql(bulk, sql_query, file_path)
        if state:
            return True, "PASS"
    except Exception as e:
        logging.info("Exception happened in the exportJobSQL: " + str(e))
        if retry_attempts > 4:
            return False, str(e)
        main(zoho_table_name, view_id, retry_attempts + 1)


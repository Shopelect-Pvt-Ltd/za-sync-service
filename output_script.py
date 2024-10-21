from config import MongoInit
from concurrent.futures import ThreadPoolExecutor
from bson import ObjectId
import logging

from pyspark.sql.functions import when,col

import os
import shutil
import glob
import json

logging.basicConfig(
    level = logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)


def export_dataframe_to_json(result_df):
    output_directory="output_temp"
    output_file = "./output2.json"

    #Create the file
    with open(output_file,"w+"):
        pass
    
    print("Attempting Export to json")
    if result_df.count() > 0:
            try:

                # Coalesce to a single partition and write to a temporary directory
                result_df.coalesce(1).write.json(output_directory, mode='overwrite')
                print(f"Data written to {output_directory} successfully.")

                # Create the final output file if it does not exist
                final_output_file = output_file

                # Move the output file to the desired output file
                part_file = os.path.join(output_directory, "part-00000-*.json")
                found_file = False
                for file in glob.glob(part_file):
                    shutil.move(file, final_output_file)
                    print(f"Moved to {final_output_file} successfully.")
                    found_file = True
                
                if not found_file:
                    print("No part file found to move.")
                
                # Remove the temporary directory
                shutil.rmtree(output_directory)
            
            except Exception as e:
                print(f"Error occurred while writing to JSON: {e}")
    else:
        print("Result DataFrame is empty. No data to write.")


def delete_collection_in_mongo(output_database,op_collection):
    client = MongoInit()
    op_db = client[output_database]
    output_collection = op_db[op_collection]

    # output_collection.delete_many({})
    output_collection.drop()

    logging.info(f"Collection {op_collection} deleted successfully")

def export_json_to_mongo(output_database,op_collection):
    client = MongoInit()
    vendor_master_db = client[output_database]
    vendor_master_db.create_collection(op_collection)


    logging.info("Collection created successfully {op_collection}")

    output_collection = vendor_master_db[op_collection]

    # Path to your JSON file
    file_path = "./output2.json"

    # Function to insert a document into MongoDB
    def insert_document(data):
        try:
            output_collection.insert_one(data)
        except Exception as e:
            try:
                id = data["_id"]
                query = {"_id":id}
                output_collection.update_one(query,{"$set":data})
            except:    
                logging.error(f"Error inserting document: {e}")

    # Function to process a batch of lines
    def process_batch(lines):
        for line in lines:
            try:
                data = json.loads(line)
                insert_document(data)
            except json.JSONDecodeError as e:
                logging.error(f"Error decoding JSON: {e}")

    # Number of threads to use
    num_threads = 10

    # Read and process the file using multiple threads
    with open(file_path, 'r') as file:
        lines = file.readlines()
        logging.info(f" Number of documents to insert {len(lines)}")

        # Split the lines into chunks for each thread
        chunk_size = len(lines) // num_threads
        chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

        # Use ThreadPoolExecutor to parallelize the insertion
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.map(process_batch, chunks)

    os.remove(file_path)
    logging.info("BIBIDEBA")
    logging.info("Data imported successfully.")


def rename_collection(output_database_name, source_collection_name, target_collection_name):
    client = MongoInit()
    op_db_cursor = client[output_database_name]
    op_coll_cursor = op_db_cursor[source_collection_name]

    op_coll_cursor.rename(target_collection_name,dropTarget=True)

    logging.info("Renamed Collection Successfully")



def export_to_mongo(df_final,output_database_name,output_collection_name):
        export_dataframe_to_json(df_final)

        export_json_to_mongo(output_database_name,f"{output_collection_name}_temp")
        rename_collection(output_database_name,f"{output_collection_name}_temp",output_collection_name)
        logging.info("Imported data successfully")
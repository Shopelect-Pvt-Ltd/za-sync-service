from config import (MongoInit,
                    SparkInit,
                    VENDOR_MASTER_DATABASE,
                    ZA_MONGO_SYNC, 
                    CLIENT_2B_DATA_ZA)
from pyspark.sql.functions import col, concat_ws, sha1, struct, cast
from pyspark.sql.types import DoubleType

from output_script import export_dataframe_to_json,export_json_to_mongo
from concurrent.futures import ThreadPoolExecutor

import logging

import os, time, json

import uuid

spark = SparkInit()

logging.basicConfig(
    level = logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)

start_time = time.time()

#View Folder
collections_folder = "collections"
#Json File Paths
za_collection_path = os.path.join(collections_folder,"za_collection.json")


client =  MongoInit()
vendor_master_db = client[VENDOR_MASTER_DATABASE]

za_collection = vendor_master_db[CLIENT_2B_DATA_ZA]

def load_collections_to_json(source_collection_names,source_collections,output_files):
    for collection_name,collection,output_file in zip(source_collection_names,source_collections,output_files):
        start_time = time.time()
        print(f"Finding collection - {collection_name}")
        cursor = collection.find()
        print("Collection found")
        data = list(cursor)
        print("Dumping file")
        with open(output_file, 'w+') as file:
            json.dump(data, file, default=str)  # Use default=str to handle non-serializable data
        end_time = time.time()
        print(f"---------- Time taken for {collection_name} was {end_time-start_time}")


def load_jsons_to_dataframes(spark):
    # Load the JSON data into DataFrames with multiLine option
    try:
        za_collection = spark.read.json("collections/za_collection.json", multiLine=True)
        print("za_collection done")
        
        if za_collection.count() == 0:
            print("hotel detalis is empty.")  

        dataframe_list =za_collection
        return dataframe_list

    except Exception as e:
        print(f"Error reading JSON files: {e}")
        spark.stop()
        exit()

def flatten_schema(schema_def):
    flattened_schema =[]
    for entry in schema_def:
        name,dtype = entry
        if dtype.startswith('struct'):
            fields_included = dtype[7:-1]
            subfields = fields_included.split(',')
            for subfield in subfields:
                sub_name, sub_type = subfield.split(":")
                field_name = f"{name}.{sub_name}"
                flattened_schema.append((field_name,sub_type.upper()))
        else :
            flattened_schema.append((name,dtype.upper()))

    return flattened_schema    


def main():
    try:
        one_off_source_collection_names = [CLIENT_2B_DATA_ZA]
        one_off_source_collections = [za_collection]
        one_off_output_files  = [za_collection_path]
        print("Loading One off collections...")
        load_collections_to_json(one_off_source_collection_names,one_off_source_collections, one_off_output_files)
        print("Oneoff collections Loaded successfully")   

        dataframe_list = load_jsons_to_dataframes(spark)

        za_collection = dataframe_list

        logging.info(f"{za_collection.count()}")
        schema_list = za_collection.dtypes

        flattened_schema = flatten_schema(schema_list)
        print(flattened_schema)

        logging.info("Schema Flattended")


        column_mapping =[]

        for column in flattened_schema:
            field_name, dtype = column
            column_mapping.append(
                {
                    "source_key":field_name,
                    "destination_key":field_name,
                    "data_type":dtype,
                    "validation_type":None,
                    "is_required":None,
                }
            )


        dict_to_append ={
            "schemaId": str(uuid.uuid1()),
            "za_table_name":"Hotel Plan Table",
            "zohoViewId":"103074000030967443",
            "column_mapping":column_mapping,
            "status":"PENDING"
        }
        with open("temp.json","w+") as tempfile:
            json.dump(dict_to_append,tempfile)

        logging.info("Schema dumped into json")

        #Record the end time
        end_time = time.time()
        # Total time
        total_time = end_time - start_time
        logging.info(f"Script executed in {total_time:.4f} seconds")

    except Exception as e:
        logging.info(f"Exception occurred in main: " + str(e))
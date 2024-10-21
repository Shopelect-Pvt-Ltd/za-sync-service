import logging
import pandas as pd
import hashlib
import psycopg2
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import numpy as np
import json
from collections import defaultdict
from pyspark.sql.functions import col,when, struct

from pyspark.sql.types import StringType,DoubleType,IntegerType

from output_script import export_to_mongo

from config import (
    MongoInit,
    SparkInit,
    ZA_MONGO_SYNC,
    HOTEL_PLAN_TABLE_ZA
)

load_dotenv()

op_database_name = os.getenv('MONGO_DATABASE')

# Setup basic configuration for logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)

def main(details):
    try:
        logging.info("Started importing...")
        # Connect to MongoDB
        client = MongoInit()
        za_mongo_sync = client[op_database_name]

        #Connect to Pyspark
        spark = SparkInit()
        logging.info('SPARK INITIALIZED SUCCESSFULLY')


        # Function to generate a unique hash ID based on row values
        def generate_hash_id(row):
            unique_str = ''.join(row.astype(str))
            hash_id = hashlib.sha256(unique_str.encode()).hexdigest()
            return hash_id

        # Load the CSV file
        file_path = f'./{details["za_table_name"]}.csv'

        df = spark.read.option("header",True).csv(file_path)

        df = df.fillna("")

        # ------------- Handling Type Casting -------------

        datatypemap = {
            "STRING":StringType(),
            "DOUBLE": DoubleType(),
            "BIGINT": IntegerType(),
            "DATE": StringType()
        }

        column_mapping = (details["column_mapping"])
        col_name_and_type=[]
        for column_map in column_mapping:
            to_append = column_map["source_key"],datatypemap[column_map["data_type"]]
            col_name_and_type.append(to_append)
        
        logging.info(col_name_and_type)

        logging.info("DF CREATED SUCCESSFULLY")



        # ------------- Handling Nesting and TypeCasting -------------

        parent_dict = {}
        for col_name,col_type in col_name_and_type:
            if "." in col_name:
                parent, child = col_name.split(".")
                typed_child = child,col_type
                parent_dict.setdefault(parent, []).append(typed_child)
            else:
                df= df.withColumn(col_name,col(col_name).cast(col_type))    


        for parent,typed_children in parent_dict.items():
            df = df.withColumn(parent,struct(*[
                (col(f"`{parent}.{child}`").cast(child_type)).alias(child) for child,child_type in typed_children
            ])
            )
            df = df.drop(*[col(f"`{parent}.{child}`") for child,child_type in typed_children]) 

        print("\n")
        logging.info(" Printing df schema")
        df.printSchema()

        export_to_mongo(df,op_database_name,details["mongo_collection_name"])
        os.remove(file_path)
        return "PASS", "PASS"
    except Exception as e:
        logging.info("Exception occurred while importJobSQL: " + str(e))
        return "FAILED", str(e)

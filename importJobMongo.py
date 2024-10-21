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

from output_script import export_json_to_mongo,export_dataframe_to_json, delete_collection_in_mongo

from config import (
    MongoInit,
    SparkInit,
    ZA_MONGO_SYNC,
    HOTEL_PLAN_TABLE_ZA
)

load_dotenv()

op_database = os.getenv('MONGO_DATABASE')

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
        za_mongo_sync = client[op_database]

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

        export_dataframe_to_json(df)
        output_collection = details["mongo_collection_name"]

        delete_collection_in_mongo(op_database,output_collection)

        export_json_to_mongo(op_database,output_collection)

        # mongo_collection_name = details["mongo_collection_name"]
        # column_data = details["column_mapping"]
        # for i in range(len(column_data)):
        #     if (column_data[i]["data_type"] == "DOUBLE" or column_data[i]["data_type"] == "BIGINT" ):
        #         numeric_columns.append(column_data[i]["source_key"])
        #     if column_data[i]["data_type"] in datatypemap:
        #         create_table_query += ' "' + column_data[i]["destination_key"] + '" ' + datatypemap[
        #             column_data[i]["data_type"]]
        #     else:
        #         create_table_query += ' "' + column_data[i]["destination_key"] + '" ' + "TEXT"

        #     insert_query_param += ' "' + column_data[i]["destination_key"] + '" '
        #     insert_values_param += ' %s'
        #     if i < len(column_data) - 1:
        #         create_table_query += ","
        #         insert_query_param += ","
        #         insert_values_param += ","

        # create_table_query += " )"
        # insert_query_param += " )"
        # insert_values_param += " ) ON CONFLICT (id) DO NOTHING;"

        # insert_query = insert_query_param + insert_values_param

        # for col in numeric_columns:
        #     df[col] = df[col].apply(lambda x: str(x).replace(',', '') if pd.notnull(x) else x).astype(float)

        # df = df.replace({np.nan: None})
        # df = df.where(pd.notnull(df), None)

        # logging.info(create_table_query)
        # logging.info(insert_query)

        # drop_table_query=f"DROP TABLE IF EXISTS public.{pg_table_name}"
        # cur.execute(drop_table_query)

        # cur.execute(create_table_query)

        # for index, row in df.iterrows():
        #     data_to_insert = [row["id"]]
        #     for i in range(len(column_data)):
        #         data_to_insert += [row[column_data[i]["source_key"]]]
        #     cur.execute(insert_query, tuple(data_to_insert))
        # conn.commit()
        # # Close the connection
        # cur.close()
        # conn.close()
        logging.info("Imported data successfully")
        return "PASS", "PASS"
    except Exception as e:
        logging.info("Exception occurred while importJobSQL: " + str(e))
        return "FAILED", str(e)

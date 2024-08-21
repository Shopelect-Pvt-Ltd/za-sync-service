import logging
import pandas as pd
import hashlib
import psycopg2
import os
from dotenv import load_dotenv
import numpy as np

load_dotenv()
# Setup basic configuration for logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)

# Database connection details
host = os.getenv("POSTGRESS_HOST")
database = os.getenv("POSTGRESS_DB")
user = os.getenv("POSTGRESS_USER")
password = os.getenv("POSTGRESS_PASSWORD")
port = os.getenv("POSTGRESS_PORT")


def main(details):
    try:
        logging.info("Started importing...")
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        cur = conn.cursor()

        # Function to generate a unique hash ID based on row values
        def generate_hash_id(row):
            unique_str = ''.join(row.astype(str))
            hash_id = hashlib.sha256(unique_str.encode()).hexdigest()
            return hash_id

        # Load the CSV file
        file_path = f'./{details["za_table_name"]}.csv'
        df = pd.read_csv(file_path)

        # Apply the function to each row to create a new column 'ID'
        df['id'] = df.apply(generate_hash_id, axis=1)

        numeric_columns = []

        datatypemap = {
            "STRING": "TEXT",
            "NUMBER": "NUMERIC",
            "DATE": "TEXT",
            "BOOLEAN": "TEXT"
        }

        pg_table_name = details["pg_table_name"]
        column_data = details["column_mapping"]
        create_table_query = f"CREATE TABLE IF NOT EXISTS public.{pg_table_name} ( id text PRIMARY KEY,"
        insert_query_param = f"INSERT INTO public.{pg_table_name} ( id,"
        insert_values_param = " VALUES (%s,"
        for i in range(len(column_data)):
            if column_data[i]["data_type"] == "NUMBER":
                numeric_columns.append(column_data[i]["source_key"])
            if column_data[i]["data_type"] in datatypemap:
                create_table_query += ' "' + column_data[i]["destination_key"] + '" ' + datatypemap[
                    column_data[i]["data_type"]]
            else:
                create_table_query += ' "' + column_data[i]["destination_key"] + '" ' + "TEXT"

            insert_query_param += ' "' + column_data[i]["destination_key"] + '" '
            insert_values_param += ' %s'
            if i < len(column_data) - 1:
                create_table_query += ","
                insert_query_param += ","
                insert_values_param += ","

        create_table_query += " )"
        insert_query_param += " )"
        insert_values_param += " ) ON CONFLICT (id) DO NOTHING;"

        insert_query = insert_query_param + insert_values_param

        for col in numeric_columns:
            df[col] = df[col].apply(lambda x: str(x).replace(',', '') if pd.notnull(x) else x).astype(float)

        df = df.replace({np.nan: None})
        df = df.where(pd.notnull(df), None)

        logging.info(create_table_query)
        logging.info(insert_query)

        cur.execute(create_table_query)

        for index, row in df.iterrows():
            data_to_insert = [row["id"]]
            for i in range(len(column_data)):
                data_to_insert += [row[column_data[i]["source_key"]]]
            cur.execute(insert_query, tuple(data_to_insert))
        conn.commit()
        # Close the connection
        cur.close()
        conn.close()
        logging.info("Imported data successfully")
        return True, "PASS"
    except Exception as e:
        logging.info("Exception occurred while importJobSQL: " + str(e))
        return False, str(e)

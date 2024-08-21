import pandas as pd
import json

# Load the CSV file
csv_file_path = '/Users/komalkantmillan/Downloads/agtable_schema.csv'
df = pd.read_csv(csv_file_path)

# Extract the headers from the CSV
headers = df.columns.tolist()

# Sample JSON structure provided by the user
json_data = {
    "_id": {
        "$oid": "66c4373d47c130d0a0dde1bc"
    },
    "schemaId": "",
    "zohoViewId": "121",
    "pg_table_name": "mmt_flight_recon_test",
    "column_mapping": []
}

# Creating the updated column_mapping using headers from the CSV
for _, row in df.iterrows():
    column_mapping_entry = {header: row[header] for header in headers}
    json_data["column_mapping"].append(column_mapping_entry)

# Convert to JSON string for better readability or further usage
updated_json_str = json.dumps(json_data, indent=4)

# Print the updated JSON
print(updated_json_str)

# Optionally, you can save the updated JSON to a file
with open('updated_json_output.json', 'w') as outfile:
    outfile.write(updated_json_str)

from google.cloud import bigquery

# Replace with your credentials path (securely manage credentials)
credentials_path = "dl-9953189580-d142d3b17505.json"

# Establish a secure connection to BigQuery
credentials, project_id = google.auth.default()
client = bigquery.Client(credentials=credentials, project=project_id)

# Specify the public dataset and table (replace with actual names)
dataset_id = "bigquery-public-data"
table_id = "google_trends.top_terms"

# Retrieve the schema of the table
table = client.get_table(f"{dataset_id}.{table_id}")
headers = [field.name for field in table.schema]

# Download the data to a temporary file (replace with desired format)
query = f"""
SELECT
*
FROM `bigquery-public-data.google_trends.top_terms`
WHERE
-- Choose only the top term each day.
refresh_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
-- Filter to the last 2 weeks.
"""
destination_file = "local_data_2days.csv"  # Replace with desired filename and format

# Open the destination file for writing
with open(destination_file, "w") as outfile:
   # Write the headers to the CSV file
   outfile.write(",".join(headers) + "\n")
   # Execute the query
   job = client.query(query)
   # Write the query results to the CSV file
   for row in job:
     # Process and write each row to the file (implement data handling)
     outfile.write(",".join(str(v) for v in row) + "\n")
print(f"Data downloaded to: {destination_file}")

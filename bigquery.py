import google.auth
from google.cloud import bigquery

# Replace with your credentials path (securely manage credentials)
credentials_path = "dl-9953189580-d142d3b17505.json"

# Establish a secure connection to BigQuery
credentials, project_id = google.auth.default()
client = bigquery.Client(credentials=credentials, project=project_id)

# Specify the public dataset and table (replace with actual names)
dataset_id = "public_dataset"
table_id = "google_trends.international"

# Download the data to a temporary file (replace with desired format)
query = f"""
SELECT *
FROM `{dataset_id}.{table_id}`
"""
destination_file = "local_data.csv"  # Replace with desired filename and format

with open(destination_file, "w") as outfile:
  job = client.query(query)
  for row in job:
    # Process and write each row to the file (implement data handling)
    outfile.write(",".join(str(v) for v in row) + "\n")
print(f"Data downloaded to: {destination_file}")

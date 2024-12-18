from dotenv import load_dotenv
import os
import requests
import csv
from google.cloud import storage

load_dotenv()

# print(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
# client = storage.Client()
#
# # List buckets
# for bucket in client.list_buckets():
#     print(bucket.name)

def api_call():
    # key = os.getenv("RAPIDAPI_KEY")
    key = "77ea61cbb4msh95a32d87728d311p160428jsneed46f28e9d7"
    # host = os.getenv("RAPIDAPI_HOST")
    host = "cricbuzz-cricket.p.rapidapi.com"
    url_stats = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen"

    headers = {
        'x-rapidapi-key': key,
        'x-rapidapi-host': host
    }

    response = requests.get(url_stats, headers=headers, params={'formatType': 'test'})
    data = response.json().get('rank', [])

    data_result = []
    for record in data:
        # Append each record as a list with rank, name, and country as separate items
        data_result.append([record.get('rank'), record.get('name'), record.get('country')])

    print("Data extracted from API......")
    return data_result



def write_csv(data):
    file_name = "ranking_file.csv"

    # Write the data into a CSV file
    if data:
        with open(file_name, 'w', newline='') as file: # Ensure 'newline' is specified for cross-platform compatibility
            writer = csv.writer(file)
            # Write the header row
            writer.writerow(['Rank', 'Name', 'Country'])
            # Write the data rows
            writer.writerows(data)
        print("Data is loaded in CSV.....")

def write_csv_gcs(local_file_path, bucket_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    # Upload the file
    blob.upload_from_filename(local_file_path)
    print(f"File {local_file_path} uploaded to bucket {bucket_name}/{destination_blob_name}.")
    return True


local_file_path = "ranking_file.csv"
bucket_name ="cricket_stats_source"
destination_blob_name = "ranking_file.csv"
# Main function to call the API and write data to CSV
api_response = api_call()
write_csv(api_response)
write_csv_gcs(local_file_path, bucket_name, destination_blob_name)


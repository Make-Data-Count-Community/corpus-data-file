import json
import csv

def json_to_csv(json_file_path, csv_file_path):
    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)

    if not data or not isinstance(data, list):
        print("Invalid or empty JSON data.")
        return

    print("Valid JSON data.")
    # Extract field names from the first object in the JSON array
    field_names = list(data[0].keys())

    with open(csv_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=field_names)
        csv_writer.writeheader()
        csv_writer.writerows(data)

    print(f"Conversion completed. CSV file saved at {csv_file_path}")

json_file_path = '/xxx/data-dump.json'
csv_file_path = '/xxx/data-dump.csv'
json_to_csv(json_file_path, csv_file_path)

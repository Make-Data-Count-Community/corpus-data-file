import os
import psycopg2
import requests
import csv
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import urllib.parse

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path, override=True)

# Database connection parameters
conn_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Connect to the database
conn = psycopg2.connect(**conn_params)
cursor = conn.cursor()

# Query to get organization names without external_id
query = """
SELECT id, title
FROM affiliations
WHERE external_id IS NULL
UNION
SELECT id, title
FROM funders
WHERE external_id IS NULL;
"""

logging.info("Executing database query to retrieve organizations...")
cursor.execute(query)
organizations = cursor.fetchall()
total_records = len(organizations)
logging.info(f"Total organizations to process: {total_records}")

# ROR API endpoint
ROR_API_URL = "https://api.ror.org/organizations?affiliation="

# Function to escape reserved characters for ElasticSearch
def escape_reserved_chars(organization_name):
    reserved_chars = ['+', '-', '=', '&&', '||', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', '/', '\\']
    for char in reserved_chars:
        organization_name = organization_name.replace(char, '%5C' + urllib.parse.quote(char))
    return organization_name

# Function to query ROR API
def query_ror_api(organization_name):
    try:
        escaped_name = escape_reserved_chars(organization_name)
        response = requests.get(ROR_API_URL + escaped_name)
        
        if response.status_code == 429:
            logging.warning("Rate limit exceeded. Waiting for slightly more than 5 minutes before retrying...")
            time.sleep(310)  # Slightly more than 5 minutes (300 seconds)
            return query_ror_api(organization_name)  # Retry after waiting
        elif response.status_code == 200:
            return response.json().get('items', [])
        else:
            logging.error(f"Failed to query ROR API for organization '{organization_name}'. Status code: {response.status_code}")
            return []
    except requests.RequestException as e:
        logging.error(f"Request exception: {e}")
        return []

# Function to process each organization
def process_organization(org_id_name):
    org_id, org_name = org_id_name
    matches = query_ror_api(org_name)
    row = [org_name]
    
    for match in matches[:5]:  # Limit to 5 matches
        row.extend([match['organization']['name'], match['organization']['id'], match['score']])
    
    # Fill in the rest of the row if fewer than 5 matches
    row.extend([""] * (15 - len(row)))  # 15 = 3 fields per match * 5 matches + 1 original name
    return row

# Create CSV file for the report
output_file = 'ror_id_candidate_matches.csv'
logging.info(f"Creating CSV report file: {output_file}")

with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write CSV header
    writer.writerow(["Original Name", "Match 1 Name", "Match 1 ID", "Match 1 Score",
                     "Match 2 Name", "Match 2 ID", "Match 2 Score",
                     "Match 3 Name", "Match 3 ID", "Match 3 Score",
                     "Match 4 Name", "Match 4 ID", "Match 4 Score",
                     "Match 5 Name", "Match 5 ID", "Match 5 Score"])
    
    # Use ThreadPoolExecutor to process organizations concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_organization, org) for org in organizations]
        for index, future in enumerate(as_completed(futures), start=1):
            try:
                row = future.result()
                writer.writerow(row)
                logging.info(f"Completed processing record {index}/{total_records}. Remaining records: {total_records - index}")
            except Exception as e:
                logging.error(f"Error processing record: {e}")

logging.info("Report generation completed.")

# Close the database connection
cursor.close()
conn.close()
logging.info("Database connection closed.")

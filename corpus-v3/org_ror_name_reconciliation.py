import os
import time
import psycopg2
import requests
import re
from psycopg2 import extras
from dotenv import load_dotenv

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(project_root, '.env')

load_dotenv(dotenv_path, override=True)

conn_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

AFFILIATIONS_TABLE = "affiliations"
FUNDERS_TABLE = "funders"
ROR_API_BASE_URL = os.getenv('ROR_API_BASE_URL')

INVALID_EXTERNAL_IDS = {
    AFFILIATIONS_TABLE: ["N/A", "null"],
    FUNDERS_TABLE: [
        "DOE BER/TES/ARM",
        "LANL - LDRD",
        "2016YFC0203302, 2016YFC0200404, 2016YFC0203300",
        "https://data.bris.ac.uk/datasets",
        "Grant agreement No 654109",
        "224308 (ARF)",
        "WFLUS008/12 (ARF) and WFLUS006/14 (ARF)",
        "W81XWH-10-1-0910 (MSB) and W81XWH-13-1-0297 (MSB)",
        "CO19772 (MSB and JCB)",
        "(BioSC)"
    ]
}

def get_db_connection():
    """Establish connection to PostgreSQL."""
    return psycopg2.connect(**conn_params)


def normalize_external_id(external_id):
    """Normalize external IDs to correct prefixes."""

    external_id = external_id.strip()

    ror_pattern = r"^0[a-zA-Z0-9]{6}\d{2}$"

    if external_id.startswith("https://ror.org/") or external_id.startswith("https://doi.org/"):
        return external_id
    elif external_id.startswith("grid."):
        return f"https://www.grid.ac/{external_id}"
    elif external_id.startswith("0000"):
        return f"http://isni.org/isni/{external_id.replace(' ', '')}"
    elif external_id.startswith("10."):
        return f"https://doi.org/{external_id}"
    elif bool(re.match(ror_pattern, external_id)):
       return f"https://ror.org/{external_id}"
    elif external_id.startswith("http://isni.org"):
        return external_id.replace("http://isni.org", "http://isni.org/isni")
    else:
        return external_id
    

def add_columns_if_not_exist(cursor, table_name):
    """Add ror_id and ror_name columns to the table."""
    print(f"Adding columns to {table_name}...")
    cursor.execute(f"""
        ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS ror_id TEXT;
        ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS ror_name TEXT;
    """)
    

def clean_invalid_external_ids(cursor, table_name, invalid_ids):
    """Remove invalid external ID values."""
    print(f"Cleaning invalid external IDs from {table_name}...")
    placeholders = ",".join(["%s"] * len(invalid_ids))
    cursor.execute(f"""
        UPDATE {table_name}
        SET external_id = NULL
        WHERE TRIM(external_id) IN ({placeholders});
    """, tuple(invalid_ids))

def fetch_ror_by_query(query):
    """Fetch ROR data by query (name or external ID)."""
    if query.startswith("https://www.grid.ac") or query.startswith("http://isni.org/isni") or query.startswith("https://doi.org"):
        query = query.rsplit('/', 1)[-1]

    response = requests.get(f"{ROR_API_BASE_URL}?query={query}")
    if response.status_code == 200:
        data = response.json()

        records = data.get("items", [])
        if records:
            organization = records[0]
            return organization.get("id"), organization.get("name")
    return None, None


def fetch_ror_by_id(ror_id):
    """Fetch ROR name by its ID."""
    response = requests.get(f"{ROR_API_BASE_URL}/{ror_id}")
    if response.status_code == 200:
        data = response.json()
        return data.get("id"), data.get("name")
    return None, None


def fetch_ror_by_title(title):
    """Fetch ROR name by its ID."""
    response = requests.get(f"{ROR_API_BASE_URL}?affiliation={title}")
    if response.status_code == 200:
        data = response.json()
        matching_item = next(
                (item for item in data.get('items', [])
                    if int(item.get('score')) == 1 and item.get('chosen', False)),
                None
            )
        if matching_item:
            return matching_item.get("organization").get("id"), matching_item.get("organization").get("name")
        return None, None
    return None, None
    

def process_table(cursor, table_name):
    """Core logic to clean, normalize, and populate ROR data in a table."""
    cursor.execute(f"SELECT id, external_id, title FROM {table_name} where ror_id IS NULL AND ror_name IS NULL;")
    rows = cursor.fetchall()
    print(f"Number of rows fetched: {len(rows)}")
    
    batch_updates = []
    batch_size = 5000
    total_processed = 0

    for count, row in enumerate(rows, start=1):
        record_id, external_id, title = row
        ror_id, ror_name = None, None

        if external_id:
            external_id = normalize_external_id(external_id)
            
            if external_id.startswith("https://ror.org/"):
                ror_id, ror_name = fetch_ror_by_id(external_id)
            else:
                ror_id, ror_name = fetch_ror_by_query(external_id)
        else:
            ror_id, ror_name = fetch_ror_by_title(title)

        batch_updates.append((external_id, ror_id, ror_name, record_id))

        print(f"Processed record {count}/{len(rows)}")

        if count % batch_size == 0:
            execute_batch_update(cursor, table_name, batch_updates)
            total_processed += len(batch_updates)
            print(f"Processed {total_processed}/{len(rows)} records")
            batch_updates.clear()
    
    if batch_updates:
        execute_batch_update(cursor, table_name, batch_updates)
        total_processed += len(batch_updates)
        print(f"Processed {total_processed}/{len(rows)} records")
        batch_updates.clear()

    print(f"Processing {table_name} complete.")


def execute_batch_update(cursor, table_name, updates):
    """Execute a batch update for the given updates."""
    query = f"""
        UPDATE {table_name}
        SET external_id = data.external_id,
            ror_id = data.ror_id,
            ror_name = data.ror_name
        FROM (VALUES %s) AS data(external_id, ror_id, ror_name, id)
        WHERE {table_name}.id = data.id::uuid;
    """
    extras.execute_values(
        cursor,
        query,
        updates,
        template="(%s, %s, %s, %s)",
    )


def process_by_titles(cursor, table_name):
    """Process the remaining records where we could not get ROR data by external ID."""
    cursor.execute(f"SELECT id, external_id, title FROM {table_name} where external_id IS NOT NULL AND ror_id IS NULL;")
    rows = cursor.fetchall()

    batch_updates = []
    batch_size = 5000
    total_processed = 0

    for count, row in enumerate(rows, start=1):
        record_id, external_id, title = row
        ror_id, ror_name = None, None

        ror_id, ror_name = fetch_ror_by_title(title)

        batch_updates.append((external_id, ror_id, ror_name, record_id))

        print(f"Processed record {count}/{len(rows)}")

        if count % batch_size == 0:
            execute_batch_update(cursor, table_name, batch_updates)
            total_processed += len(batch_updates)
            print(f"Processed {total_processed}/{len(rows)} records")
            batch_updates.clear()

        time.sleep(1)
    
    if batch_updates:
        execute_batch_update(cursor, table_name, batch_updates)
        total_processed += len(batch_updates)
        print(f"Processed {total_processed}/{len(rows)} records")
        batch_updates.clear()

    print(f"Processing {table_name} complete.")
        

def main():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        for table_name in [AFFILIATIONS_TABLE, FUNDERS_TABLE]:
            print(f"Processing {table_name}...")
            add_columns_if_not_exist(cursor, table_name)
            clean_invalid_external_ids(cursor, table_name, INVALID_EXTERNAL_IDS[table_name])
            process_table(cursor, table_name)
            process_by_titles(cursor, table_name)
            conn.commit()
        print("Processing completed successfully.")
    except Exception as e:
        print("Error:", e)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
import os
import psycopg2
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

def get_db_connection():
    """Establish connection to PostgreSQL."""
    return psycopg2.connect(**conn_params)


def normalize_dois(doi):
    """
    Normalize DOI to the desired format: https://doi.org/10.XXXXX/XXXXXX
    """
    if not doi:
        return None
    doi = doi.strip()
    if doi.startswith("https://doi.org/"):
        return doi
    elif doi.startswith("doi.org/"):
        return f"https://{doi}"
    elif doi.startswith("10."):
        return f"https://doi.org/{doi}"
    else:
        return doi


def process_table(cursor):
    cursor.execute(f"SELECT id, subj_id, obj_id, dataset, publication, doi FROM assertions;")
    rows = cursor.fetchall()
    print(f"Number of rows fetched: {len(rows)}")
    
    batch_updates = []
    batch_size = 5000
    total_processed = 0

    for count, row in enumerate(rows, start=1):
        id, subj_id, obj_id, dataset, publication, doi = row
        
        normalized_doi = normalize_dois(doi)
        normalized_subj_id = normalize_dois(subj_id)
        normalized_obj_id = normalize_dois(obj_id)
        normalized_dataset = normalize_dois(dataset)
        normalized_publication = normalize_dois(publication)


        batch_updates.append((normalized_subj_id, normalized_obj_id, normalized_doi, normalized_publication, normalized_dataset, id))

        if count % batch_size == 0:
            execute_batch_update(cursor, batch_updates)
            total_processed += len(batch_updates)
            print(f"Processed {total_processed}/{len(rows)} records")
            batch_updates.clear()
    
    if batch_updates:
        execute_batch_update(cursor, batch_updates)
        total_processed += len(batch_updates)
        print(f"Processed {total_processed}/{len(rows)} records")
        batch_updates.clear()

    print(f"Processing assertions complete.")


def execute_batch_update(cursor, updates):
    """Execute a batch update for the given updates."""
    query = """
        UPDATE assertions
        SET subj_id = data.subj_id,
            obj_id = data.obj_id,
            doi = data.doi,
            publication = data.publication,
            dataset = data.dataset
        FROM (VALUES %s) AS data(subj_id, obj_id, doi, publication, dataset, id)
        WHERE assertions.id = data.id::uuid;
    """
    extras.execute_values(
        cursor,
        query,
        updates,
        template="(%s, %s, %s, %s, %s, %s)",
    )

def main():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        process_table(cursor)
        conn.commit()
        print("Processing completed successfully.")
    except Exception as e:
        print("Error:", e)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
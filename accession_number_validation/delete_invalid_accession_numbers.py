import os
import pandas as pd
import psycopg2
import re
from psycopg2 import sql
from dotenv import load_dotenv

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(project_root, '.env')

load_dotenv(dotenv_path, override=True)

conn_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

script_dir = os.path.dirname(os.path.abspath(__file__))
csv_directory = os.path.join(script_dir, 'assertions_to_delete')
BATCH_SIZE = 1000

def connect_db():
    """Establishes a connection to the database."""
    try:
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = False
        return conn
    except psycopg2.DatabaseError as e:
        print(f"Error connecting to the database: {e}")
        return None

def delete_related_assertions(conn, repo_id, file_path):
    try:
        with conn.cursor() as cursor:
            cursor.execute("BEGIN;")
            
            # Create a temporary table for assertion IDs
            cursor.execute("CREATE TEMPORARY TABLE temp_assertions (id UUID PRIMARY KEY);")
            
            # Import data into the temporary table
            with open(file_path, 'r') as f:
                cursor.copy_expert(sql.SQL("COPY temp_assertions (id) FROM STDIN WITH CSV HEADER"), f)
            
            # Verify data in temp_assertions
            check_temp_assertions_count(cursor)

            # Get the total number of assertions before deletion
            total_before = get_total_assertions(cursor)
            print(f"Total assertions in repo_id {repo_id} before deletion: {total_before}")

            # Perform deletions using JOIN
            cursor.execute("""
                DELETE FROM assertions_affiliations
                USING temp_assertions
                WHERE assertions_affiliations.assertion_id = temp_assertions.id;
            """)
            print("Deleted records from assertions_affiliations")

            cursor.execute("""
                DELETE FROM assertions_funders
                USING temp_assertions
                WHERE assertions_funders.assertion_id = temp_assertions.id;
            """)
            print("Deleted records from assertions_funders")

            cursor.execute("""
                DELETE FROM assertions_subjects
                USING temp_assertions
                WHERE assertions_subjects.assertion_id = temp_assertions.id;
            """)
            print("Deleted records from assertions_subjects")

            cursor.execute("""
                DELETE FROM assertions
                USING temp_assertions
                WHERE assertions.id = temp_assertions.id;
            """)
            print("Deleted records from assertions")

            conn.commit()
            print(f"Successfully deleted records for repo_id {repo_id}")

            # Verify remaining assertions
            total_after = get_total_assertions(cursor)
            print(f"Total assertions in repo_id {repo_id} after deletion: {total_after}")

            cursor.execute("DROP TABLE IF EXISTS temp_assertions;")
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error deleting records for repo_id {repo_id}: {e}")

def check_temp_assertions_count(cursor):
    cursor.execute("SELECT COUNT(*) FROM temp_assertions;")
    count = cursor.fetchone()[0]
    print(f"Total records in temp_assertions: {count}")

def check_csv_file(file_path):
    df = pd.read_csv(file_path)
    if df.empty:
        print(f"Warning: {file_path} is empty.")
    else:
        print(f"{file_path} contains {len(df)} records.")

def get_total_assertions(cursor):
    cursor.execute("SELECT COUNT(*) FROM assertions;")
    total = cursor.fetchone()[0]
    return total


def process_csv_files(directory):
    """
    Processes all CSV files in the given directory.

    :param directory: The directory containing the CSV files.
    """
    conn = connect_db()
    if conn is None:
        return

    try:
        for filename in os.listdir(directory):
            if '-remove' in filename:
                # Use regex to extract the repository ID from the filename
                repo_id = re.split(r'-remove.*\.', filename)[0]
                file_path = os.path.join(directory, filename)

                print(f"Processing {file_path} for repo_id {repo_id}")
                delete_related_assertions(conn, repo_id, file_path)
    finally:
        conn.close()

process_csv_files(csv_directory)

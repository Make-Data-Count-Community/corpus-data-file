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
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

script_dir = os.path.dirname(os.path.abspath(__file__))
csv_directory = os.path.join(script_dir, 'assertions_to_delete')

def connect_db():
    """Establishes a connection to the database."""
    try:
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = False
        return conn
    except psycopg2.DatabaseError as e:
        print(f"Error connecting to the database: {e}")
        return None

def delete_related_assertions(conn, assertion_ids):
    """Deletes assertions and related data from the database."""
    assertion_ids = tuple(assertion_ids)
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE TEMPORARY TABLE assertions_affiliations_temp AS SELECT * FROM assertions_affiliations;")
            cursor.execute("CREATE TEMPORARY TABLE assertions_funders_temp AS SELECT * FROM assertions_funders;")
            cursor.execute("CREATE TEMPORARY TABLE assertions_subjects_temp AS SELECT * FROM assertions_subjects;")
            print("Created TEMP tables for assertions_affiliations, assertions_funders, and assertions_subjects.")

            cursor.execute(
                sql.SQL("CREATE TEMPORARY TABLE assertions_temp AS SELECT * FROM assertions WHERE id NOT IN %s;"),
                [assertion_ids]
            )
            print(f"Created temp table for assertions, excluding {len(assertion_ids)} records.")

            cursor.execute("TRUNCATE TABLE assertions CASCADE;")
            print("Truncated assertions table.")

            cursor.execute("INSERT INTO assertions SELECT * FROM assertions_temp;")
            print("Repopulated assertions table with clean records.")

            cursor.execute("INSERT INTO assertions_affiliations SELECT * FROM assertions_affiliations_temp WHERE assertion_id IN (SELECT id FROM assertions);")
            cursor.execute("INSERT INTO assertions_funders SELECT * FROM assertions_funders_temp WHERE assertion_id IN (SELECT id FROM assertions);")
            cursor.execute("INSERT INTO assertions_subjects SELECT * FROM assertions_subjects_temp WHERE assertion_id IN (SELECT id FROM assertions);")
            print("Repopulated assertions_affiliations, assertions_funders, and assertions_subjects tables.")

            cursor.execute("DROP TABLE assertions_temp;")
            cursor.execute("DROP TABLE assertions_affiliations_temp;")
            cursor.execute("DROP TABLE assertions_funders_temp;")
            cursor.execute("DROP TABLE assertions_subjects_temp;")
            print("Dropped temp tables.")

        conn.commit()
        print("Transaction committed successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Error during transaction, rolling back. Details: {e}")
        raise

def process_csv_files(directory):
    """Processes all CSV files in the given directory."""
    conn = connect_db()
    if conn is None:
        return

    assertion_ids = []

    try:
        for filename in os.listdir(directory):
            if '-remove' in filename:
                repo_id = re.split(r'-remove.*\.', filename)[0]
                file_path = os.path.join(directory, filename)
                df = pd.read_csv(file_path, header=None)

                file_assertion_ids = df.iloc[:, 0].tolist()
                assertion_ids.extend(file_assertion_ids)

        if assertion_ids:
            print(f"Processing {len(assertion_ids)} assertion IDs...")
            delete_related_assertions(conn, assertion_ids)
        else:
            print("No assertion IDs found in the specified directory.")

    finally:
        conn.close()
        print("Database connection closed.")

process_csv_files(csv_directory)

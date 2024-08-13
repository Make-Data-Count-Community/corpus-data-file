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

def delete_related_assertions(conn, repo_id, assertion_ids):
    """
    Deletes assertions and related records from the database.

    :param conn: The database connection.
    :param repo_id: The repository ID from the filename.
    :param assertion_ids: A list of assertion IDs to delete.
    """
    try:
        with conn.cursor() as cursor:
            for i in range(0, len(assertion_ids), BATCH_SIZE):
                ids_tuple = tuple(assertion_ids[i:i+BATCH_SIZE])

                print(f"Deleting {len(assertion_ids)} assertions and related records for repo_id {repo_id}")

                cursor.execute(
                    sql.SQL("DELETE FROM assertions_affiliations WHERE assertion_id IN %s"),
                    [ids_tuple]
                )
                cursor.execute(
                    sql.SQL("DELETE FROM assertions_funders WHERE assertion_id IN %s"),
                    [ids_tuple]
                )
                cursor.execute(
                    sql.SQL("DELETE FROM assertions_subjects WHERE assertion_id IN %s"),
                    [ids_tuple]
                )

                cursor.execute(
                    sql.SQL("DELETE FROM assertions WHERE id IN %s"),
                    [ids_tuple]
                )

                conn.commit()
                print(f"Deleted {len(assertion_ids)} assertions and related records for repo_id {repo_id}")
    except Exception as e:
        conn.rollback()
        print(f"Error deleting records for repo_id {repo_id}: {e}")

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

                df = pd.read_csv(file_path, header=None)

                assertion_ids = df.iloc[:, 0].tolist()
                print(f"Processing {len(assertion_ids)} assertions for repo_id {repo_id}")
                delete_related_assertions(conn, repo_id, assertion_ids)
    finally:
        conn.close()

process_csv_files(csv_directory)

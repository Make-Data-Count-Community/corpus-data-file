import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os
from uuid import uuid4
from datetime import datetime
import logging
from multiprocessing import Pool, cpu_count
from itertools import repeat
from io import StringIO

# Load the environment variables from the .env file
load_dotenv()

# Retrieve the environment variables
dbname = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('PG_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection details
conn_params = {
    'dbname': dbname,
    'user': user,
    'password': password,
    'host': host,
    'port': port
}

# Subjects to be inserted
subjects = [
    'Biological sciences',
    'Health biotechnology',
    'Animal and dairy science',
    'Physical sciences',
    'Chemical sciences',
    'Basic medicine'
]

# Mapping of repository IDs to subjects
repository_subject_mapping = {
    '00363b65-f3ef-4fa9-8255-23ab269f4930': ['Biological sciences', 'Basic medicine'],
    '87646104-e5ef-494b-b2f3-a46c9572e003': ['Biological sciences', 'Basic medicine', 'Physical sciences', 'Chemical sciences'],
    '6087b2e9-ecbf-4898-8047-5f484f1bce2f': ['Biological sciences', 'Basic medicine'],
    'b2a4aa2b-db3f-456a-8e2b-7d935343385e': ['Biological sciences', 'Basic medicine'],
    '1edec4bf-cfee-4296-8893-d1b0ca528f92': ['Biological sciences', 'Basic medicine'],
    '58d689da-7c8c-4ac1-90c9-69253d28f81f': ['Biological sciences', 'Basic medicine'],
    '5f36c68f-bb46-4a21-9b95-6bb87de12aa0': ['Biological sciences', 'Basic medicine'],
    '8d9c72f8-7b96-4b5c-86b0-b3f0dd7d0b0d': ['Biological sciences', 'Basic medicine', 'Animal and dairy science'],
    '31ffd918-669b-4d61-9470-784226277b5b': ['Biological sciences', 'Basic medicine'],
    '19ad31a7-e6d0-4547-ad14-1201d3c96dca': ['Biological sciences', 'Basic medicine'],
    '524e4405-f959-4e3c-ab4e-eecaa8ed23d5': ['Biological sciences', 'Basic medicine'],
    '1f463165-6957-491b-a1e1-e484540200f0': ['Basic medicine', 'Chemical sciences', 'Biological sciences'],
    '79760077-45df-4626-9675-60ee459aa283': ['Biological sciences', 'Basic medicine'],
    'b5966ef4-8bd3-4de8-aafb-396df8e75b0b': ['Biological sciences', 'Basic medicine'],
    '8748538d-965e-4440-85cc-d9d1722e7ca9': ['Biological sciences', 'Basic medicine'],
    '66807551-597e-4088-9743-32690481f6ff': ['Biological sciences', 'Basic medicine'],
    'b4440b59-ca28-4a67-a65f-2dc02fb0aa23': ['Biological sciences', 'Basic medicine'],
    'f43825eb-5b72-4f1a-b716-dc7eec6d4206': ['Biological sciences', 'Basic medicine'],
    'c908c286-c01b-44c7-bac9-3bd53148d898': ['Biological sciences', 'Basic medicine'],
    '345977e0-6fb8-476e-9742-0b8987e2fce8': ['Basic medicine', 'Chemical sciences', 'Biological sciences'],
    '0a60b1a9-041a-444e-bd6a-94caaab7591b': ['Biological sciences', 'Basic medicine', 'Health biotechnology'],
    '876f8718-c8b2-4d05-95ff-ab00ba6fa56b': ['Biological sciences', 'Basic medicine', 'Health biotechnology', 'Chemical sciences'],
    '23b16007-328b-46a1-84ee-19cd32995091': ['Biological sciences', 'Basic medicine'],
    '7109c7ca-dd40-4dfc-a883-c3bd2ad93ea5': ['Biological sciences', 'Basic medicine'],
    '75800c1d-b982-4542-b996-033781f70fa1': ['Biological sciences', 'Basic medicine'],
    '4371937f-226a-4381-b07a-1ccc0085f0fd': ['Biological sciences', 'Basic medicine'],
    'ffe70f6b-6db0-402e-86b7-cc07bacbbdc8': ['Biological sciences', 'Basic medicine'],
    '0ccefa3f-5c25-4191-945e-715ce1816f57': ['Biological sciences', 'Basic medicine'],
    '69bb4f86-1d75-487e-971a-f446a2ef0792': ['Biological sciences', 'Basic medicine'],
    '490f73dd-7532-453b-bd37-a96a566d60ba': ['Biological sciences', 'Basic medicine'],
    'e387c003-d7e5-455d-b7b0-544e9251b1d0': ['Biological sciences', 'Basic medicine'],
    'a4855df9-755e-41d6-bddf-4589461e303c': ['Basic medicine', 'Chemical sciences', 'Biological sciences'],
    'c82c6040-e644-4d94-a54f-97f0236c7147': ['Biological sciences', 'Basic medicine'],
    'c56fbe18-f93b-478b-9674-00056bdeb887': ['Biological sciences', 'Basic medicine'],
    'd201d41a-2b84-4ebc-91a4-afab8c481944': ['Biological sciences', 'Basic medicine'],
    '54753603-c263-4cc0-bd65-57c39b5a20f6': ['Biological sciences', 'Basic medicine'],
    'e5960008-7a81-46b1-b526-d7dbea7e2c93': ['Biological sciences', 'Basic medicine'],
    'd3ee57d1-bce4-437d-b054-e686d9abc727': ['Basic medicine', 'Chemical sciences', 'Biological sciences']
}

def get_connection():
    return psycopg2.connect(**conn_params)

def fetch_subject_ids():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, title FROM subjects 
        WHERE title IN %s;
    """, (tuple(subject.lower() for subject in subjects),))
    subject_ids = {row[1]: row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return subject_ids

def insert_batch(batch):
    conn = get_connection()
    cur = conn.cursor()
    cur.copy_expert(sql.SQL("""
        COPY assertions_subjects (id, type, created, updated, assertion_id, subject_id, inferred)
        FROM STDIN WITH CSV
    """), batch)
    conn.commit()
    cur.close()
    conn.close()

def process_repository(repo_id, subject_ids):
    conn = get_connection()
    cur = conn.cursor()
    logger.info(f"Processing repository_id: {repo_id}")
    cur.execute("""
        SELECT id FROM assertions
        WHERE repository_id = %s;
    """, (repo_id,))
    assertion_ids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    batch = StringIO()
    for assertion_id in assertion_ids:
        for subject in repository_subject_mapping[repo_id]:
            subject_id = subject_ids.get(subject.lower())
            if subject_id:
                batch.write(f"{uuid4()},assertionSubject,{datetime.now()},{datetime.now()},{assertion_id},{subject_id},False\n")
                if batch.tell() >= 10 * 1024 * 1024:  # Flush every 10MB
                    batch.seek(0)
                    insert_batch(batch)
                    batch = StringIO()

    if batch.tell() > 0:
        batch.seek(0)
        insert_batch(batch)

def main():
    subject_ids = fetch_subject_ids()
    
    pool = Pool(cpu_count())
    pool.starmap(process_repository, zip(repository_subject_mapping.keys(), repeat(subject_ids)))
    pool.close()
    pool.join()

    logger.info("All insertions completed successfully.")

if __name__ == "__main__":
    main()

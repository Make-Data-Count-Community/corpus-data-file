import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os
from uuid import uuid4
from datetime import datetime
import logging
from io import StringIO

# Load the environment variables from the .env file
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection details
conn_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Subjects to be inserted
subjects = [
    'health biotechnology',
    'animal and dairy science',
    'basic medicine'
]

# Mapping of repository IDs to subjects
repository_subject_mapping = {
    '00363b65-f3ef-4fa9-8255-23ab269f4930': ['biological sciences', 'basic medicine'],
    '6087b2e9-ecbf-4898-8047-5f484f1bce2f': ['biological sciences', 'basic medicine'],
    'b2a4aa2b-db3f-456a-8e2b-7d935343385e': ['biological sciences', 'basic medicine'],
    '1edec4bf-cfee-4296-8893-d1b0ca528f92': ['biological sciences', 'basic medicine'],
    '58d689da-7c8c-4ac1-90c9-69253d28f81f': ['biological sciences', 'basic medicine'],
    '5f36c68f-bb46-4a21-9b95-6bb87de12aa0': ['biological sciences', 'basic medicine'],
    '8d9c72f8-7b96-4b5c-86b0-b3f0dd7d0b0d': ['biological sciences', 'basic medicine', 'Animal and dairy science'],
    '31ffd918-669b-4d61-9470-784226277b5b': ['biological sciences', 'basic medicine'],
    '19ad31a7-e6d0-4547-ad14-1201d3c96dca': ['biological sciences', 'basic medicine'],
    '524e4405-f959-4e3c-ab4e-eecaa8ed23d5': ['biological sciences', 'basic medicine'],
    '1f463165-6957-491b-a1e1-e484540200f0': ['basic medicine', 'chemical sciences', 'biological sciences'],
    '79760077-45df-4626-9675-60ee459aa283': ['biological sciences', 'basic medicine'],
    'b5966ef4-8bd3-4de8-aafb-396df8e75b0b': ['biological sciences', 'basic medicine'],
    '8748538d-965e-4440-85cc-d9d1722e7ca9': ['biological sciences', 'basic medicine'],
    '66807551-597e-4088-9743-32690481f6ff': ['biological sciences', 'basic medicine'],
    'b4440b59-ca28-4a67-a65f-2dc02fb0aa23': ['biological sciences', 'basic medicine'],
    'f43825eb-5b72-4f1a-b716-dc7eec6d4206': ['biological sciences', 'basic medicine'],
    'c908c286-c01b-44c7-bac9-3bd53148d898': ['biological sciences', 'basic medicine'],
    '345977e0-6fb8-476e-9742-0b8987e2fce8': ['basic medicine', 'chemical sciences', 'biological sciences'],
    '0a60b1a9-041a-444e-bd6a-94caaab7591b': ['biological sciences', 'basic medicine', 'Health biotechnology'],
    '876f8718-c8b2-4d05-95ff-ab00ba6fa56b': ['biological sciences', 'basic medicine', 'Health biotechnology', 'chemical sciences'],
    '23b16007-328b-46a1-84ee-19cd32995091': ['biological sciences', 'basic medicine'],
    '7109c7ca-dd40-4dfc-a883-c3bd2ad93ea5': ['biological sciences', 'basic medicine'],
    '75800c1d-b982-4542-b996-033781f70fa1': ['biological sciences', 'basic medicine'],
    '4371937f-226a-4381-b07a-1ccc0085f0fd': ['biological sciences', 'basic medicine'],
    'ffe70f6b-6db0-402e-86b7-cc07bacbbdc8': ['biological sciences', 'basic medicine'],
    '0ccefa3f-5c25-4191-945e-715ce1816f57': ['biological sciences', 'basic medicine'],
    '69bb4f86-1d75-487e-971a-f446a2ef0792': ['biological sciences', 'basic medicine'],
    '490f73dd-7532-453b-bd37-a96a566d60ba': ['biological sciences', 'basic medicine'],
    'e387c003-d7e5-455d-b7b0-544e9251b1d0': ['biological sciences', 'basic medicine'],
    'a4855df9-755e-41d6-bddf-4589461e303c': ['basic medicine', 'chemical sciences', 'biological sciences'],
    'c82c6040-e644-4d94-a54f-97f0236c7147': ['biological sciences', 'basic medicine'],
    'c56fbe18-f93b-478b-9674-00056bdeb887': ['biological sciences', 'basic medicine'],
    'd201d41a-2b84-4ebc-91a4-afab8c481944': ['biological sciences', 'basic medicine'],
    '54753603-c263-4cc0-bd65-57c39b5a20f6': ['biological sciences', 'basic medicine'],
    'e5960008-7a81-46b1-b526-d7dbea7e2c93': ['biological sciences', 'basic medicine'],
    '87646104-e5ef-494b-b2f3-a46c9572e003': ['biological sciences', 'basic medicine', 'physical sciences', 'chemical sciences'],
    'd3ee57d1-bce4-437d-b054-e686d9abc727': ['basic medicine', 'chemical sciences', 'biological sciences']
}

def get_connection():
    return psycopg2.connect(**conn_params)

def add_unique_constraint():
    """Adds a unique constraint to the title column in the subjects table if it does not already exist."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Check if the constraint already exists
        cur.execute("""
            SELECT 1 
            FROM pg_constraint 
            WHERE conname = 'unique_title';
        """)
        if cur.fetchone():
            logger.info("Unique constraint already exists on subjects table.")
        else:
            # Add the unique constraint
            cur.execute("""
                ALTER TABLE subjects
                ADD CONSTRAINT unique_title UNIQUE (title);
            """)
            conn.commit()
            logger.info("Unique constraint added to subjects table.")
    except Exception as e:
        logger.error(f"Error adding unique constraint: {e}")
    finally:
        cur.close()
        conn.close()

def lowercase_subject_titles():
    """Converts all subject titles in the subjects table to lowercase."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("UPDATE subjects SET title = LOWER(title);")
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Converted all subject titles to lowercase.")

def insert_missing_subjects():
    """Inserts subjects into the database if they do not exist."""
    conn = get_connection()
    cur = conn.cursor()
    for subject in subjects:
        cur.execute("""
            INSERT INTO subjects (id, title, type, created, updated)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (title) DO NOTHING;
        """, (
            str(uuid4()), 
            subject.lower(), 
            'subject',
            datetime.now(),
            datetime.now()
        ))
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Inserted missing subjects into the database.")


def fetch_subject_ids(subjects):
    """Fetches the subject IDs for the given subject titles from the database."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, title FROM subjects 
        WHERE title IN %s;
    """, (tuple(subjects),))
    subject_ids = [row[0] for row in cur.fetchall()]
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

    # Fetch assertion IDs
    cur.execute("""
        SELECT id FROM assertions
        WHERE repository_id = %s;
    """, (repo_id,))
    assertion_ids = [row[0] for row in cur.fetchall()]

    # Fetch existing mappings
    existing_mappings_query = """
    SELECT assertion_id, subject_id
    FROM assertions_subjects
    WHERE assertion_id = ANY(ARRAY[{}]::uuid[]) AND subject_id = ANY(ARRAY[{}]::uuid[]);
    """.format(
        ','.join(f"'{id}'" for id in assertion_ids),
        ','.join(f"'{id}'" for id in subject_ids)
    )
    cur.execute(existing_mappings_query)
    existing_mappings = set((row[0], row[1]) for row in cur.fetchall())
    cur.close()
    conn.close()

    batch = StringIO()
    for assertion_id in assertion_ids:
        for subject_id in subject_ids:
            if (assertion_id, subject_id) not in existing_mappings:
                batch.write(f"{uuid4()},assertionSubject,{datetime.now()},{datetime.now()},{assertion_id},{subject_id},False\n")
                if batch.tell() >= 10 * 1024 * 1024:  # Flush every 10MB
                    batch.seek(0)
                    insert_batch(batch)
                    batch = StringIO()

    if batch.tell() > 0:
        batch.seek(0)
        insert_batch(batch)

def main():
    add_unique_constraint()
    insert_missing_subjects()
    lowercase_subject_titles()

    for repository, subjects in repository_subject_mapping.items():
        subject_ids = fetch_subject_ids(subjects)
        process_repository(repository, subject_ids)

    logger.info("All insertions completed successfully.")

if __name__ == "__main__":
    main()

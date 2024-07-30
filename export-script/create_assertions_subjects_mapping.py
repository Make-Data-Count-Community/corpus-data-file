import os
import psycopg2
import pandas as pd
import re
import time
from dotenv import load_dotenv
from psycopg2 import sql

# Load environment variables
load_dotenv()

# Database connection details
conn_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Define a hash mapping repository_id to subject_ids
repository_subjects = {
    "00363b65-f3ef-4fa9-8255-23ab269f4930": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "87646104-e5ef-494b-b2f3-a46c9572e003": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61",  # basic medicine
        "d55d310a-7e8c-4fec-917c-966e8f817ee6",  # physical sciences
        "d0868346-66b9-433f-9d67-ed01ef5924e5"   # chemical sciences
    ],
    "6087b2e9-ecbf-4898-8047-5f484f1bce2f": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "b2a4aa2b-db3f-456a-8e2b-7d935343385e": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "1edec4bf-cfee-4296-8893-d1b0ca528f92": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "58d689da-7c8c-4ac1-90c9-69253d28f81f": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "5f36c68f-bb46-4a21-9b95-6bb87de12aa0": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "8d9c72f8-7b96-4b5c-86b0-b3f0dd7d0b0d": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61",  # basic medicine
        "53b70cbc-06f2-40c2-85e8-126f17f28740"   # animal and dairy science
    ],
    "31ffd918-669b-4d61-9470-784226277b5b": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "19ad31a7-e6d0-4547-ad14-1201d3c96dca": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "524e4405-f959-4e3c-ab4e-eecaa8ed23d5": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "1f463165-6957-491b-a1e1-e484540200f0": [
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61",  # basic medicine
        "d0868346-66b9-433f-9d67-ed01ef5924e5",  # chemical sciences
        "84fe679f-156c-4090-b25e-f752b3f8ea92"   # biological sciences
    ],
    "79760077-45df-4626-9675-60ee459aa283": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "b5966ef4-8bd3-4de8-aafb-396df8e75b0b": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "8748538d-965e-4440-85cc-d9d1722e7ca9": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "66807551-597e-4088-9743-32690481f6ff": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "b4440b59-ca28-4a67-a65f-2dc02fb0aa23": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "f43825eb-5b72-4f1a-b716-dc7eec6d4206": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "c908c286-c01b-44c7-bac9-3bd53148d898": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "345977e0-6fb8-476e-9742-0b8987e2fce8": [
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61",  # basic medicine
        "d0868346-66b9-433f-9d67-ed01ef5924e5",  # chemical sciences
        "84fe679f-156c-4090-b25e-f752b3f8ea92"   # biological sciences
    ],
    "0a60b1a9-041a-444e-bd6a-94caaab7591b": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61",  # basic medicine
        "4aba4454-594e-489c-ba96-ee0b181c98a7"   # health biotechnology
    ],
    "876f8718-c8b2-4d05-95ff-ab00ba6fa56b": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61",  # basic medicine
        "4aba4454-594e-489c-ba96-ee0b181c98a7",  # health biotechnology
        "d0868346-66b9-433f-9d67-ed01ef5924e5"   # chemical sciences
    ],
    "23b16007-328b-46a1-84ee-19cd32995091": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "7109c7ca-dd40-4dfc-a883-c3bd2ad93ea5": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "75800c1d-b982-4542-b996-033781f70fa1": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "4371937f-226a-4381-b07a-1ccc0085f0fd": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "ffe70f6b-6db0-402e-86b7-cc07bacbbdc8": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "0ccefa3f-5c25-4191-945e-715ce1816f57": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "69bb4f86-1d75-487e-971a-f446a2ef0792": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "490f73dd-7532-453b-bd37-a96a566d60ba": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "e387c003-d7e5-455d-b7b0-544e9251b1d0": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "a4855df9-755e-41d6-bddf-4589461e303c": [
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61",  # basic medicine
        "d0868346-66b9-433f-9d67-ed01ef5924e5",  # chemical sciences
        "84fe679f-156c-4090-b25e-f752b3f8ea92"   # biological sciences
    ],
    "c82c6040-e644-4d94-a54f-97f0236c7147": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "c56fbe18-f93b-478b-9674-00056bdeb887": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "d201d41a-2b84-4ebc-91a4-afab8c481944": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "54753603-c263-4cc0-bd65-57c39b5a20f6": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "e5960008-7a81-46b1-b526-d7dbea7e2c93": [
        "84fe679f-156c-4090-b25e-f752b3f8ea92",  # biological sciences
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61"   # basic medicine
    ],
    "d3ee57d1-bce4-437d-b054-e686d9abc727": [
        "9122f222-ff6e-4e51-b1be-ef5e1be55d61",  # basic medicine
        "d0868346-66b9-433f-9d67-ed01ef5924e5",  # chemical sciences
        "84fe679f-156c-4090-b25e-f752b3f8ea92"   # biological sciences
    ]
}

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    for repository_id, subject_ids in repository_subjects.items():
        # Get all assertion_ids for the current repository_id
        cursor.execute(
            sql.SQL("SELECT id FROM assertions WHERE repository_id = %s"),
            [repository_id]
        )
        assertion_ids = cursor.fetchall()

        for assertion_id in assertion_ids:
            for subject_id in subject_ids:
                # Insert into assertions_subjects
                cursor.execute(
                    sql.SQL("INSERT INTO assertions_subjects (assertion_id, subject_id) VALUES (%s, %s)"),
                    [assertion_id[0], subject_id]
                )

    # Commit the changes
    conn.commit()

except Exception as error:
    print(f"Error: {error}")
    if conn:
        conn.rollback()
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
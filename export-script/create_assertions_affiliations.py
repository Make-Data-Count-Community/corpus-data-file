import os
import uuid
import logging
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Load .env and DB Credentials ---
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path, override=True)

conn_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
}

CSV_PATH = os.path.join(os.path.expanduser("~"), "Downloads", "ror_id_candidate_matches_merged_local.csv")

# --- Helpers ---
def is_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except:
        return False

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

# --- Step 1: Load CSV ---
logging.info("🔄 Loading CSV...")
try:
    df = pd.read_csv(
        CSV_PATH,
        sep=",",
        quotechar='"',
        skipinitialspace=True,
        dtype=str,
        keep_default_na=False,
        na_values=[""],
        index_col=False  # ⛔ Ensures 'Dataset' doesn't become the index
    )
    df.columns = df.columns.str.strip()
    logging.info(f"📊 Loaded {len(df)} rows.")
except Exception as e:
    logging.error(f"❌ Failed to read CSV: {e}")
    exit(1)

# --- Step 2: Extract selected match ---
def extract_chosen_affiliation(row):
    for i in range(1, 4):
        if str(row.get(f"Match {i} Chosen", "")).strip().upper() == 'TRUE':
            return pd.Series({
                'dataset_key': row.get('Dataset', '').strip(),
                'ror_id': row.get(f'Match {i} ID', '').strip(),
                'title': row.get(f'Match {i} Name', '').strip()
            })
    return pd.Series({'dataset_key': None, 'ror_id': None, 'title': None})

df_extracted = df.apply(extract_chosen_affiliation, axis=1)
df_extracted = df_extracted.dropna(subset=['dataset_key', 'ror_id', 'title']).drop_duplicates()

logging.info(f"✅ Found {len(df_extracted)} rows with a chosen match.")
print(df_extracted.head(10).to_dict(orient="records"))

# --- Step 3: Connect to DB ---
conn = psycopg2.connect(**conn_params)
cursor = conn.cursor()

# --- Step 4: Fetch existing affiliations ---
affiliation_id_map = {}
titles = df_extracted['title'].dropna().unique().tolist()

logging.info("🔍 Fetching existing affiliations by title...")
for chunk in chunked(titles, 10000):
    cursor.execute("SELECT id, title FROM affiliations WHERE title IN %s", (tuple(chunk),))
    for aid, title in cursor.fetchall():
        affiliation_id_map[title] = aid

# --- Step 5: Insert missing affiliations ---
missing_titles = set(titles) - set(affiliation_id_map.keys())
logging.info(f"➕ Missing affiliations to insert: {len(missing_titles)}")

if missing_titles:
    new_affs = [
        (
            title,
            df_extracted[df_extracted['title'] == title]['ror_id'].iloc[0],
            "affiliation"
        )
        for title in missing_titles
    ]
    execute_values(
        cursor,
        "INSERT INTO affiliations (title, ror_id, type) VALUES %s RETURNING id, title",
        new_affs
    )
    for aid, title in cursor.fetchall():
        affiliation_id_map[title] = aid
    conn.commit()
    logging.info("✅ Inserted missing affiliations.")

# --- Step 6: Fetch assertion IDs ---
uuid_keys = df_extracted[df_extracted['dataset_key'].apply(is_uuid)]['dataset_key'].unique().tolist()
text_keys = df_extracted[~df_extracted['dataset_key'].apply(is_uuid)]['dataset_key'].unique().tolist()

assertion_id_map = {}

logging.info("🔍 Fetching assertions by UUID id...")
for chunk in chunked(uuid_keys, 10000):
    cursor.execute("""
        SELECT id, id::text FROM assertions
        WHERE id IN %s
        AND NOT EXISTS (
            SELECT 1 FROM assertions_affiliations aa WHERE aa.assertion_id = assertions.id
        )
    """, (tuple(chunk),))
    for aid, key in cursor.fetchall():
        assertion_id_map[key] = aid

logging.info("🔍 Fetching assertions by dataset field...")
for chunk in chunked(text_keys, 10000):
    cursor.execute("""
        SELECT id, dataset FROM assertions
        WHERE dataset IN %s
        AND NOT EXISTS (
            SELECT 1 FROM assertions_affiliations aa WHERE aa.assertion_id = assertions.id
        )
    """, (tuple(chunk),))
    for aid, key in cursor.fetchall():
        assertion_id_map[key] = aid

logging.info(f"✅ Total assertions to map: {len(assertion_id_map)}")

# --- Step 7: Prepare mappings to insert ---
rows_to_insert = []
for _, row in df_extracted.iterrows():
    assertion_id = assertion_id_map.get(row['dataset_key'])
    affiliation_id = affiliation_id_map.get(row['title'])
    if assertion_id and affiliation_id:
        rows_to_insert.append((str(uuid.uuid4()), assertion_id, affiliation_id, 'assertionAffiliation'))


logging.info(f"🧾 Prepared {len(rows_to_insert)} assertion-affiliation mappings to insert.")

# --- Step 8: Insert mappings ---
if rows_to_insert:
    for chunk in chunked(rows_to_insert, 50000):
        execute_values(
            cursor,
            "INSERT INTO assertions_affiliations (id, assertion_id, affiliation_id, type) VALUES %s",
            chunk
        )
        conn.commit()
    logging.info(f"✅ Inserted {len(rows_to_insert)} assertion-affiliation mappings.")
else:
    logging.warning("⚠️ No rows to insert.")

# --- Cleanup ---
cursor.close()
conn.close()
logging.info("🏁 Done. DB connection closed.")

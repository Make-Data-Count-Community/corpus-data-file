import os
import psycopg2
import pandas as pd
import re
import time
from dotenv import load_dotenv

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(project_root, '.env')

load_dotenv(dotenv_path)

conn_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

dir_name = 'accession_number_validation_data'

start_time_total = time.time()

if not os.path.exists(dir_name):
    os.makedirs(dir_name)

repository_regex_map = {
    "00363b65-f3ef-4fa9-8255-23ab269f4930": r"^[A-Z]+[0-9]+(\.\d+)?$",
    "87646104-e5ef-494b-b2f3-a46c9572e003": r"^[0-9][A-Za-z0-9]{3}$",
    "6087b2e9-ecbf-4898-8047-5f484f1bce2f": r"^rs\d+$",
    "b2a4aa2b-db3f-456a-8e2b-7d935343385e": r"^G(PL|SM|SE|DS)\d+$",
    "1edec4bf-cfee-4296-8893-d1b0ca528f92": r"^(((WP|AC|AP|NC|NG|NM|NP|NR|NT|NW|XM|XP|XR|YP|ZP)_\d+)|(NZ\_[A-Z]{2,4}\d+))(\.\d+)?$",
    "58d689da-7c8c-4ac1-90c9-69253d28f81f": r"^([A-N,R-Z][0-9]([A-Z][A-Z, 0-9][A-Z, 0-9][0-9]){1,2})|([O,P,Q][0-9][A-Z, 0-9][A-Z, 0-9][A-Z, 0-9][0-9])(\.\d+)?$",
    "5f36c68f-bb46-4a21-9b95-6bb87de12aa0": r"^PRJ[DEN][A-Z]\d+$",
    "8d9c72f8-7b96-4b5c-86b0-b3f0dd7d0b0d": r"^((ENS[FPTG]\d{11}(\.\d+)?)|(FB\w{2}\d{7})|(Y[A-Z]{2}\d{3}[a-zA-Z](\-[A-Z])?)|([A-Z_a-z0-9]+(\.)?(t)?(\d+)?([a-z])?))$",
    "19ad31a7-e6d0-4547-ad14-1201d3c96dca": r"^PF\d{5}$",
    "524e4405-f959-4e3c-ab4e-eecaa8ed23d5": r"^[AEP]-\w{4}-\d+$",
    "1f463165-6957-491b-a1e1-e484540200f0": r"^\d+$",
    "79760077-45df-4626-9675-60ee459aa283": r"^GCA_[0-9]{9}(\.[0-9]+)?$",
    "b5966ef4-8bd3-4de8-aafb-396df8e75b0b": r"^EMD-\d{4,5}$",
    "8748538d-965e-4440-85cc-d9d1722e7ca9": r"^[A-Z]+[0-9]+$",
    "66807551-597e-4088-9743-32690481f6ff": r"^IPR\d{6}$",
    "b4440b59-ca28-4a67-a65f-2dc02fb0aa23": r"^phs[0-9]{6}(.v\d+.p\d+)?$",
    "f43825eb-5b72-4f1a-b716-dc7eec6d4206": r"^S-[A-Z]{4}[\-\_A-Z\d]+$",
    "c908c286-c01b-44c7-bac9-3bd53148d898": r"^[1-6]\.[0-9]+\.[0-9]+\.[0-9]+$",
    "345977e0-6fb8-476e-9742-0b8987e2fce8": r"^CPX-[0-9]+$",
    "0a60b1a9-041a-444e-bd6a-94caaab7591b": r"(^R-[A-Z]{3}-\d+(-\d+)?(\.\d+)?$)|(^REACT_\d+(\.\d+)?$)",
}

conn = psycopg2.connect(**conn_params)
cur = conn.cursor()

for repository_id, regex_pattern in repository_regex_map.items():
    start_time_repo = time.time()
    print(f"Processing repository_id: {repository_id}")

    query = f"""
        SELECT 
            a.id,
            a.subj_id,
            a.obj_id,
            a.type,
            a.created,
            a.updated,
            a.source_type,
            a.title,
            a.accession_number,
            a.published_date,
            a.doi,
            a.not_found,
            a.retried,
            r.title AS repository_title, 
            p.title AS publisher_title, 
            src.title AS source_title
        FROM 
            assertions a
        LEFT JOIN repositories r ON a.repository_id = r.id
        LEFT JOIN publishers p ON a.publisher_id = p.id
        LEFT JOIN sources src ON a.source_id = src.id
        WHERE 
            a.repository_id = '{repository_id}'
            AND source_id='c66aafc0-cfd6-4bce-9235-661a4a7c6126' 
        """
    cur.execute(query)
    rows = cur.fetchall()

    print(f"Number of rows fetched: {len(rows)}")

    colnames = [desc[0] for desc in cur.description]

    df = pd.DataFrame(rows, columns=colnames)
    
    df['accession_number_pattern_match'] = df['accession_number'].apply(lambda x: bool(re.match(regex_pattern, x)))
    
    csv_filename = f"accession_number_validation_data/{repository_id}.csv"
    df.to_csv(csv_filename, index=False)

    print(f"CSV file created: {csv_filename}")
    end_time_repo = time.time()
    minutes, seconds = divmod(end_time_repo - start_time_repo, 60)
    print(f"Time taken for repository_id {repository_id}: {int(minutes)} minutes {int(seconds)} seconds")

end_time_total = time.time()
minutes, seconds = divmod(end_time_total - start_time_total, 60)
print(f"Total time taken: {int(minutes)} minutes {int(seconds)} seconds")
cur.close()
conn.close()
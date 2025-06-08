# Data Citation Corpus

This project generates data dumps in JSON and CSV formats.

## Requirements

Before running this project, please ensure that you have the following requirements installed on your machine:

- **PostgreSQL**: You will need to have PostgreSQL installed. If you don't have it installed, you can download it from the official website: [PostgreSQL](https://www.postgresql.org/)
- **Python 3**: You will need to have Python 3 installed. If you don't have it installed, you can download it from the official website: [Python](https://www.python.org/)

## Setup

To set up the project, follow these steps:

1. **Clone the repository**: 
    ```bash
    git clone git@github.com:datacite/corpus-data-file.git
    ```
2. **Navigate to the project directory**: 
    ```bash
    cd corpus-data-file
    ```
3. **Create a `.env` file**: 
    ```bash
    cp .env.example .env
    ```
    Add your database credentials in the `.env` file.

## How to Run Scripts

### Make Scripts Executable

To make the scripts executable, run:
```bash
chmod +x ./export-script/create_assertion_formatted_table.sh
```

### Create Table with Formatted Data

Run the script to create a table with formatted data:
```bash
./export-script/create_assertion_formatted_table.sh
```

### Generate Dump Files

Run the script to generate the data dump files:
```bash
./export-script/generate_assertion_details.sh
```

## Process Behind Generating Dump Files

1. **Create Multiple SQL Queries**: 
    Create [multiple SQL queries](https://github.com/datacite/corpus-data-file/blob/main/sql-queries/assertion_details_multiple_queries.sql) to create a table and populate it with related formatted data following the [spec document](https://docs.google.com/document/d/1mIbsjr_RFUj3sqJ4LaWEhSAzWkL50blIhttuhAgyWXg/edit#heading=h.etz4yswwhta9).

2. **Automate Table Creation**: 
    Create a bash script [create_assertion_formatted_table.sh](https://github.com/datacite/corpus-data-file/blob/main/export-script/create_assertion_formatted_table.sh) to automate the creation of the table.

3. **Generate Data Dump Files**: 
    Create a bash script [generate_assertion_details.sh](https://github.com/datacite/corpus-data-file/blob/main/export-script/generate_assertion_details.sh) to generate the data dump files. This will create JSON dump files from the formatted table and convert each individual file to CSV using a Python script [convert_to_csv.py](https://github.com/datacite/corpus-data-file/blob/main/export-script/convert_to_csv.py) following the [spec document](https://docs.google.com/document/d/1mIbsjr_RFUj3sqJ4LaWEhSAzWkL50blIhttuhAgyWXg/edit#heading=h.etz4yswwhta9).

## Accession Number Validation

This script is used to validate the accession numbers in our database against a set of regular expressions for each repository.

### Setup

1. Ensure you have Python 3 installed on your system.
2. Navigate to the script directory:
    ```bash
    cd accession_number_validation
    ```
3. Install the required Python packages:
    ```bash
    pip install -r requirements.txt
    ```
4. Create a `.env` file in the project root and add database credentials:
    ```bash
    touch .env
    ```
    Open the `.env` file and add the following lines:
    ```bash
    DB_NAME=<database_name>
    DB_USER=<database_username>
    DB_PASSWORD=<database_password>
    DB_HOST=<database_host>
    DB_PORT=<database_port>
    ```

### Running the Script

To run the `accession_number_validation.py` script, use the following command:
```bash
python accession_number_validation.py
```

## Data Removal Specifications

## Data Removal Specifications

This section outlines the steps and SQL queries used to clean and refine the dataset by removing duplicate, non-citation, clinical trial, orphaned records and invalid citations. The aim is to ensure the integrity and relevance of the dataset by retaining only the most pertinent and accurate data points.

### Initial Assertion Records
- **Total Initial Records**: 10,006,058

To count the initial records:
```bash
select count(*) from public.assertions;
```

### Delete Duplicate Assertions
- **Duplicates Removed**: 4,110,019
- **Total Records After Removal**: 5,896,039

Query to delete duplicates:
```sql
BEGIN;

DROP TABLE IF EXISTS assertions_affiliations_temp;
CREATE TEMPORARY TABLE assertions_affiliations_temp AS
SELECT * FROM assertions_affiliations;

DROP TABLE IF EXISTS assertions_funders_temp;
CREATE TEMPORARY TABLE assertions_funders_temp AS
SELECT * FROM assertions_funders;

DROP TABLE IF EXISTS assertions_subjects_temp;
CREATE TEMPORARY TABLE assertions_subjects_temp AS
SELECT * FROM assertions_subjects;

DROP TABLE IF EXISTS ranked_assertions;
CREATE TEMPORARY TABLE ranked_assertions AS
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY obj_id, subj_id, repository_id, publisher_id, journal_id, accession_number, source_id ORDER BY updated DESC) AS rn
    FROM assertions;

TRUNCATE TABLE assertions CASCADE;

INSERT INTO assertions (id, type, created, updated, activity_id, repository_id, publisher_id, journal_id, source_type, title, obj_id, subj_id, published_date, accession_number, doi, relation_type_id, source_id, not_found, retried)
SELECT id, type, created, updated, activity_id, repository_id, publisher_id, journal_id, source_type, title, obj_id, subj_id, published_date, accession_number, doi, relation_type_id, source_id, not_found, retried
FROM ranked_assertions
WHERE rn = 1;

INSERT INTO assertions_affiliations
SELECT *
FROM assertions_affiliations_temp
WHERE assertion_id IN (SELECT id FROM assertions);

INSERT INTO assertions_subjects
SELECT *
FROM assertions_subjects_temp
WHERE assertion_id IN (SELECT id FROM assertions);

INSERT INTO assertions_funders
SELECT *
FROM assertions_funders_temp
WHERE assertion_id IN (SELECT id FROM assertions);

DROP TABLE ranked_assertions;
DROP TABLE assertions_affiliations_temp;
DROP TABLE assertions_funders_temp;
DROP TABLE assertions_subjects_temp;

COMMIT;
```

### Delete Non-Citation Records
- **Non-Citation Records Removed**: 273,567
- **Total Records After Removal**: 5,622,472

Query to delete non-citation records:
```sql
BEGIN;

DROP TABLE IF EXISTS assertions_affiliations_temp;
CREATE TEMPORARY TABLE assertions_affiliations_temp AS
SELECT * FROM assertions_affiliations;

DROP TABLE IF EXISTS assertions_funders_temp;
CREATE TEMPORARY TABLE assertions_funders_temp AS
SELECT * FROM assertions_funders;

DROP TABLE IF EXISTS assertions_subjects_temp;
CREATE TEMPORARY TABLE assertions_subjects_temp AS
SELECT * FROM assertions_subjects;

DROP TABLE IF EXISTS citation_relation_types_assertions;
CREATE TEMPORARY TABLE citation_relation_types_assertions AS
    SELECT *
    FROM public.assertions
    WHERE relation_type_id is null
        OR relation_type_id in (
            'cites',
            'is-cited-by',
            'references',
            'is-referenced-by',
            'is-supplemented-by',
            'is-supplement-to'
        );

TRUNCATE TABLE assertions CASCADE;

INSERT INTO assertions
SELECT *
FROM citation_relation_types_assertions;

INSERT INTO assertions_affiliations
SELECT *
FROM assertions_affiliations_temp
WHERE assertion_id IN (SELECT id FROM assertions);

INSERT INTO assertions_subjects
SELECT *
FROM assertions_subjects_temp
WHERE assertion_id IN (SELECT id FROM assertions);

INSERT INTO assertions_funders
SELECT *
FROM assertions_funders_temp
WHERE assertion_id IN (SELECT id FROM assertions);

DROP TABLE citation_relation_types_assertions;
DROP TABLE assertions_affiliations_temp;
DROP TABLE assertions_funders_temp;
DROP TABLE assertions_subjects_temp;

COMMIT;
```

### Delete Clinical Trials Assertions
- **Clinical Trials Records Removed**: 28,334
- **Total Records After Removal**: 5,594,138

Query to delete clinical trials data:
```sql
BEGIN;

DROP TABLE IF EXISTS assertions_affiliations_temp;
CREATE TEMPORARY TABLE assertions_affiliations_temp AS
SELECT * FROM assertions_affiliations;

DROP TABLE IF EXISTS assertions_funders_temp;
CREATE TEMPORARY TABLE assertions_funders_temp AS
SELECT * FROM assertions_funders;

DROP TABLE IF EXISTS assertions_subjects_temp;
CREATE TEMPORARY TABLE assertions_subjects_temp AS
SELECT * FROM assertions_subjects;

DROP TABLE IF EXISTS non_clinical_trial_assertions;
CREATE TEMPORARY TABLE non_clinical_trial_assertions AS
    SELECT *
    FROM assertions
	WHERE repository_id NOT IN ('fef75a3c-6e48-4170-be9d-415601efb689', '2638e611-ff6f-49db-9b3e-702ecd16176b') OR repository_id IS NULL;

TRUNCATE TABLE assertions CASCADE;

INSERT INTO assertions
SELECT *
FROM non_clinical_trial_assertions;

INSERT INTO assertions_affiliations
SELECT *
FROM assertions_affiliations_temp
WHERE assertion_id IN (SELECT id FROM assertions);

INSERT INTO assertions_subjects
SELECT *
FROM assertions_subjects_temp
WHERE assertion_id IN (SELECT id FROM assertions);

INSERT INTO assertions_funders
SELECT *
FROM assertions_funders_temp
WHERE assertion_id IN (SELECT id FROM assertions);

DROP TABLE non_clinical_trial_assertions;
DROP TABLE assertions_affiliations_temp;
DROP TABLE assertions_funders_temp;
DROP TABLE assertions_subjects_temp;

COMMIT;
```

### Delete Orphaned Records
- **Funders Removed (without related assertions)**: 1,003
- **Subjects Removed (without related assertions)**: 0
- **Affiliations Removed (without related assertions)**: 6,199

Query to delete orphaned records:
```sql
BEGIN;

DELETE FROM funders
WHERE id NOT IN (
	SELECT funder_id FROM assertions_funders
);

DELETE FROM subjects
WHERE id NOT IN (
	SELECT subject_id FROM assertions_subjects
);

DELETE FROM affiliations
WHERE id NOT IN (
	SELECT affiliation_id FROM assertions_affiliations
);

COMMIT;
```
### Delete Rows where sub_id = obj_id, Invalid citations
- **Records Removed**: 44,097
- **Total Records After Removal**: 5,550,041

Query to delete invalid citations:
```sql
BEGIN;

DROP TABLE IF EXISTS assertions_affiliations_temp;
CREATE TEMPORARY TABLE assertions_affiliations_temp AS
SELECT * FROM assertions_affiliations;

DROP TABLE IF EXISTS assertions_funders_temp;
CREATE TEMPORARY TABLE assertions_funders_temp AS
SELECT * FROM assertions_funders;

DROP TABLE IF EXISTS assertions_subjects_temp;
CREATE TEMPORARY TABLE assertions_subjects_temp AS
SELECT * FROM assertions_subjects;

DROP TABLE IF EXISTS subj_id_obj_id;
CREATE TEMPORARY TABLE subj_id_obj_id AS
    SELECT *
    FROM assertions
	WHERE subj_id <> obj_id;

TRUNCATE TABLE assertions CASCADE;

INSERT INTO assertions
SELECT *
FROM subj_id_obj_id;

INSERT INTO assertions_affiliations
SELECT *
FROM assertions_affiliations_temp
WHERE assertion_id IN (SELECT id FROM assertions);

INSERT INTO assertions_subjects
SELECT *
FROM assertions_subjects_temp
WHERE assertion_id IN (SELECT id FROM assertions);

INSERT INTO assertions_funders
SELECT *
FROM assertions_funders_temp
WHERE assertion_id IN (SELECT id FROM assertions);

DROP TABLE subj_id_obj_id;
DROP TABLE assertions_affiliations_temp;
DROP TABLE assertions_funders_temp;
DROP TABLE assertions_subjects_temp;

COMMIT;
```

### Summary of Data Removal

- **Initial Total Records**: 10,006,058
- **Duplicates Removed**: 4,110,019
- **Non-Citation Records Removed**: 273,567
- **Clinical Trials Records Removed**: 28,334
- **Funders Removed (without related assertions)**: 1,003
- **Subjects Removed (without related assertions)**: 0
- **Affiliations Removed (without related assertions)**: 6,199
- **Invalid Citations Removed**: 44,097
- **Final Total Records**: 5,550,041

## V4 Updates: EuropePMC Data Processing

### Overview
Version 4 introduces support for processing EuropePMC dataset citations through automated data collection, mapping, and standardization.

### Features
- Automated downloading of EuropePMC dataset citation files
- PMCID to DOI mapping using multiple sources
- Repository name standardization
- Rate-limited API interactions with both EuropePMC and DataCite
- Persistent caching to improve performance on subsequent runs

### Setup and Usage

#### Step 1: Download EuropePMC Data
Run the downloader script to fetch all necessary files:
```bash
chmod +x ./corpus-v4/eupmc_file_downloader.sh
./corpus-v4/eupmc_file_downloader.sh
```

This script will:
- Create a directory for raw data (`europepmc_raw_data`)
- Download CSV files from EuropePMC TextMinedTerms
- Download XML files from EuropePMC PMCXMLData
- Download and extract the PMID-PMCID-DOI mapping file

#### Step 2: Process the Downloaded Data
Run the Python script to process the files:
```bash
cd corpus-v4
python eupmc_reformat_csv.py
```

The script processes each CSV file to:
1. Map PMCIDs to DOIs using:
   - The downloaded DOI mapping file
   - A local cache of previous API responses
   - The EuropePMC API (with rate limiting)
2. Standardize repository names using the provided mapping file
3. Generate formatted CSV files with columns:
   - `repository`: Standardized repository name
   - `dataset`: Dataset ID
   - `publication`: Full DOI URL format (https://doi.org/10.XXXX/XXXXX)

### File Descriptions
- `eupmc_file_downloader.sh`: Downloads necessary files from EuropePMC
- `eupmc_reformat_csv.py`: Processes downloaded CSVs and creates formatted output
- `repository_mapping.json`: Maps repository codes to standardized names

### Output
The script generates formatted CSV files in the `europepmc_processed_data` directory, with one file per repository dataset. It also maintains an API cache to improve performance on subsequent runs.
```

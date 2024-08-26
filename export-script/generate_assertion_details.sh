#!/bin/bash

# This script generates assertion details from the PostgreSQL database and saves them to JSON files.
# The script is designed to run on a local machine and requires the following:
# - psql (PostgreSQL client) installed on the local machine
# - Access to the PostgreSQL database (host, name, user, and password)
# - .env file in the root directory with values set for $DB_NAME, $DB_HOST, $DB_USER and $DB_PASSWORD

SCRIPT_PARENT_DIR=$( cd "$( dirname "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" )" && pwd )

ENV_FILE="$SCRIPT_PARENT_DIR/.env"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: .env file not found in $SCRIPT_PARENT_DIR, copy the .env.example file to .env and update the values."
    exit 1
fi

source "$ENV_FILE"

if [[ -z "$DB_HOST" || -z "$DB_USER" || -z "$DB_NAME" || -z "$DB_PASSWORD" ]]; then
    echo "Error: Required environment variables are not set in the .env file"
    exit 1
fi

QUERY_PREFIX="SELECT json_agg(t) FROM (SELECT * FROM assertion_details_formatted ORDER BY id OFFSET "
QUERY_SUFFIX=" LIMIT 1000000) t;"
MAIN_OUTPUT_DIR="$SCRIPT_PARENT_DIR/data-citation-corpus-v2.0-output"
JSON_OUTPUT_DIR="$MAIN_OUTPUT_DIR/json"
CSV_OUTPUT_DIR="$MAIN_OUTPUT_DIR/csv"
CURRENT_DATE=$(date +%Y-%m-%d)
CHUNK_SIZE=1000000
TOTAL_RECORDS=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -tA -c "SELECT COUNT(*) FROM assertion_details_formatted;")
FILE_NUMBER=0
JSON_ZIP_FILENAME="${CURRENT_DATE}-data-citation-corpus-v2.0-json.zip"
CSV_ZIP_FILENAME="${CURRENT_DATE}-data-citation-corpus-v2.0-csv.zip"
PYTHON_SCRIPT="$SCRIPT_PARENT_DIR/export-script/convert_to_csv.py"

# Check if there was an error with psql connection
if ! [[ "$TOTAL_RECORDS" -gt 0 ]]; then
  echo "Error: Failed to get total records from the database. Exiting."
  exit 1
fi

echo "Total records to process: $TOTAL_RECORDS"

start_time=$(date +%s)

mkdir -p "$JSON_OUTPUT_DIR"
mkdir -p "$CSV_OUTPUT_DIR"

function run_query_and_save_json() {
    local offset=$1
    local file_number_padded=$(printf "%02d" "$FILE_NUMBER")
    echo "Processing chunk starting from $offset"
    local output_file="$JSON_OUTPUT_DIR/${CURRENT_DATE}-data-citation-corpus-${file_number_padded}-v2.0.json"
    local query="$QUERY_PREFIX$offset$QUERY_SUFFIX"
    local start_time=$(date +%s)
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -tA -c "SET CLIENT_ENCODING TO 'UTF8'; $query" -o "$output_file"
    local end_time=$(date +%s)
    local elapsed_time=$((end_time - start_time))
    local hours=$((elapsed_time / 3600))
    local minutes=$(( (elapsed_time % 3600) / 60 ))
    local seconds=$((elapsed_time % 60))
    echo "Query took $hours hours, $minutes minutes, and $seconds seconds."

    # convert json file to csv in the background
    python3 "$PYTHON_SCRIPT" "$output_file" "$CSV_OUTPUT_DIR" &
}

echo "Starting the process.."
offset=0
while [ "$offset" -lt "$TOTAL_RECORDS" ]; do
    FILE_NUMBER=$((FILE_NUMBER + 1))
    run_query_and_save_json "$offset"
    offset=$((offset + CHUNK_SIZE))
done

wait # wait for all background processes to finish

echo "Zipping the JSON files"
cd "$JSON_OUTPUT_DIR" || exit 1
zip -r "$MAIN_OUTPUT_DIR/$JSON_ZIP_FILENAME" ./*.json || exit 1
rm ./*.json

echo "Zipping the CSV files"
cd "$CSV_OUTPUT_DIR" || exit 1
zip -r "$MAIN_OUTPUT_DIR/$CSV_ZIP_FILENAME" ./*.csv || exit 1
rm ./*.csv

end_time=$(date +%s)

total_elapsed_time=$((end_time - start_time))

total_hours=$((total_elapsed_time / 3600))
total_minutes=$(( (total_elapsed_time % 3600) / 60 ))
total_seconds=$((total_elapsed_time % 60))

echo "Total time taken by the entire script: $total_hours hours, $total_minutes minutes, and $total_seconds seconds."

#!/bin/bash

# This script generates assertion details from the PostgreSQL database and saves them to JSON files.
# The script is designed to run on a local machine and requires the following:
# - psql (PostgreSQL client) installed on the local machine
# - Access to the PostgreSQL database (host, name, user, and password)

# Prompt the user for the PostgreSQL password
read -s -p "Enter the PostgreSQL password: " PG_PASSWORD
echo

DB_HOST="corpus-dev-dump-2024-05-06.cpcwgoa3uzw1.eu-west-1.rds.amazonaws.com"
DB_NAME="datacite"
DB_USER="postgres"
QUERY_PREFIX="SELECT json_agg(t) FROM (SELECT * FROM assertion_details_formatted ORDER BY id OFFSET "
QUERY_SUFFIX=" LIMIT 10) t;"
OUTPUT_DIR="data-citation-corpus-v1.1-output"
CURRENT_DATE=$(date +%Y-%m-%d)
CHUNK_SIZE=10
TOTAL_RECORDS=100 #$(PGPASSWORD="$PG_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -tA -c "SELECT COUNT(*) FROM assertion_details_formatted;")
FILE_NUMBER=0
ZIP_FILENAME="${CURRENT_DATE}-data-citation-corpus-v1.1-json.zip"

echo "Total records to process: $TOTAL_RECORDS"

start_time=$(date +%s)

mkdir -p "$OUTPUT_DIR"

function run_query_and_save_json() {
    local offset=$1
    local file_number_padded=$(printf "%02d" "$FILE_NUMBER")
    echo "Processing chunk starting from $offset"
    local output_file="$OUTPUT_DIR/${CURRENT_DATE}-data-citation-corpus-${file_number_padded}-v1.1.json"
    local query="$QUERY_PREFIX$offset$QUERY_SUFFIX"
    local start_time=$(date +%s)
    PGPASSWORD="$PG_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -tA -c "SET CLIENT_ENCODING TO 'UTF8'; $query" -o "$output_file"
    local end_time=$(date +%s)
    local elapsed_time=$((end_time - start_time))
    local hours=$((elapsed_time / 3600))
    local minutes=$(( (elapsed_time % 3600) / 60 ))
    local seconds=$((elapsed_time % 60))
    echo "Query took $hours hours, $minutes minutes, and $seconds seconds."
}

echo "Starting the process.."
offset=0
while [ "$offset" -lt "$TOTAL_RECORDS" ]; do
    FILE_NUMBER=$((FILE_NUMBER + 1))
    run_query_and_save_json "$offset"
    offset=$((offset + CHUNK_SIZE))
done

echo "Zipping the JSON files"
zip -r "$ZIP_FILENAME" "$OUTPUT_DIR"/*.json

end_time=$(date +%s)

total_elapsed_time=$((end_time - start_time))

total_hours=$((total_elapsed_time / 3600))
total_minutes=$(( (total_elapsed_time % 3600) / 60 ))
total_seconds=$((total_elapsed_time % 60))

echo "Total time taken by the entire script: $total_hours hours, $total_minutes minutes, and $total_seconds seconds."

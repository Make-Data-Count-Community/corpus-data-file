#!/bin/bash

# This script creates assertion details from the PostgreSQL database.
# The script is designed to run on a local machine and requires the following:
# - psql (PostgreSQL client) installed on the local machine
# - Access to the PostgreSQL database (host, name, user, and password)
# - .env file in the root directory with values set for $DB_NAME, $DB_HOST, $DB_USER and $PG_PASSWORD

SCRIPT_PARENT_DIR=$( cd "$( dirname "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" )" && pwd )

ENV_FILE="$SCRIPT_PARENT_DIR/.env"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: .env file not found in $SCRIPT_PARENT_DIR, copy the .env.example file to .env and update the values."
    exit 1
fi

source "$ENV_FILE"

if [[ -z "$DB_HOST" || -z "$DB_USER" || -z "$DB_NAME" || -z "$PG_PASSWORD" ]]; then
    echo "Error: Required environment variables are not set in the .env file"
    exit 1
fi

FILE_PATH="$SCRIPT_PARENT_DIR/sql-queries/assertion_details_multiple_queries.sql"

start_time=$(date +%s)

echo "creating assertion details table in the database..."
PGPASSWORD="$PG_PASSWORD" psql -f "$FILE_PATH" -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME"
echo "assertion details table created successfully."

end_time=$(date +%s)

total_elapsed_time=$((end_time - start_time))
total_hours=$((total_elapsed_time / 3600))
total_minutes=$(( (total_elapsed_time % 3600) / 60 ))
total_seconds=$((total_elapsed_time % 60))

echo "Total time taken by the entire script: $total_hours hours, $total_minutes minutes, and $total_seconds seconds."

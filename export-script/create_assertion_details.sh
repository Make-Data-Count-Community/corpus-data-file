#!/bin/bash

# This script generates assertion details from the PostgreSQL database and saves them to JSON files.
# The script is designed to run on a local machine and requires the following:
# - psql (PostgreSQL client) installed on the local machine
# - Access to the PostgreSQL database (host, name, user, and password)

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

echo "creating assertion details table in the database..."
PGPASSWORD="$PG_PASSWORD" psql -f "$FILE_PATH" -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME"
echo "assertion details table created successfully."
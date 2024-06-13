# Data Citation Corpus

This project generates data dumps in JSON and CSV formats.

## Requirements

Before running this project, please ensure that you have the following requirements installed on your machine:

- PostgreSQL: You will need to have PostgreSQL installed. If you don't have it installed, you can download it from the official website: [PostgreSQL](https://www.postgresql.org/)

- Python 3: You will need to have Python 3 installed. If you don't have it installed, you can download it from the official website: [Python](https://www.python.org/)

## Setup

To set up the project, follow these steps:

1. Clone the repository: `git clone git@github.com:datacite/corpus-data-file.git`
2. Navigate to the project directory: `cd corpus-data-file`
3. Create a `.env` file, `cp .env.example .env`, and add database credentials

## How to run script

### Make scripts executable
eg. `chmod +x ./export-script/create_assertion_formatted_table.sh`

### Create table with formatted data
`./export-script/create_assertion_formatted_table.sh`

### Generate dump files
`./export-script/generate_assertion_details.sh`

## Process behind generating dump files

-  Create [multiple SQL queries ](https://github.com/datacite/corpus-data-file/blob/main/sql-queries/assertion_details_multiple_queries.sql) to create a table and populate it with related fomarmatted data following the [spec document](https://docs.google.com/document/d/1mIbsjr_RFUj3sqJ4LaWEhSAzWkL50blIhttuhAgyWXg/edit#heading=h.etz4yswwhta9).
-  Create a bash script[ create_assertion_formatted_table.sh ](https://github.com/datacite/corpus-data-file/blob/main/export-script/create_assertion_formatted_table.sh) to automate the creation of the table.
-  Create a bash script [generate_assertion_details.sh](https://github.com/datacite/corpus-data-file/blob/main/export-script/generate_assertion_details.sh) to generate the data dump files.
This will create a JSON dump files from the fomarmatted table which we created using this bash script [ create_assertion_formatted_table.sh ](https://github.com/datacite/corpus-data-file/blob/main/export-script/create_assertion_formatted_table.sh) and convert each individual file to CSV using a Python script [convert_to_csv.py ](https://github.com/datacite/corpus-data-file/blob/main/export-script/convert_to_csv.py)
 following the [spec document](https://docs.google.com/document/d/1mIbsjr_RFUj3sqJ4LaWEhSAzWkL50blIhttuhAgyWXg/edit#heading=h.etz4yswwhta9).

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
#!/usr/bin/env python3

import os
import csv
import glob
import json
import requests
import time
from datetime import datetime
import pandas as pd


def main():
    print(f"Processing EuropePMC data... {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    input_dir = "europepmc_raw_data"
    output_dir = "europepmc_processed_data"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    doi_mappings = load_doi_mappings(os.path.join(input_dir, "PMID_PMCID_DOI.csv"))
    print(f"Loaded {len(doi_mappings)} DOI mappings")

    repository_mappings = load_repository_mappings()

    cache_file = os.path.join(output_dir, "api_cache.json")
    api_cache = load_api_cache(cache_file)
    print(f"Loaded API cache with {len(api_cache)} entries")

    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
    csv_files = [f for f in csv_files if not f.endswith("PMID_PMCID_DOI.csv")]

    for csv_file in csv_files:
        base_name = os.path.basename(csv_file)
        repo_name = base_name.split('.')[0]
        standard_repo_name = repository_mappings.get(repo_name, repo_name)

        print(f"Processing {base_name} (Repository: {standard_repo_name})...")

        output_file = os.path.join(output_dir, f"{repo_name}_formatted.csv")

        process_csv_file(csv_file, output_file, doi_mappings, standard_repo_name, api_cache)

    save_api_cache(api_cache, os.path.join(output_dir, "api_cache.json"))
    print(f"Saved API cache with {len(api_cache)} entries")
    print(f"Processing complete! {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def load_api_cache(cache_file):
    """Load previously cached API responses."""
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading API cache: {e}")
            return {}
    return {}


def save_api_cache(cache, cache_file):
    """Save API cache to a file for future use."""
    try:
        with open(cache_file, 'w') as f:
            json.dump(cache, f)
    except Exception as e:
        print(f"Error saving API cache: {e}")


def load_doi_mappings(mapping_file):
    """Load the PMID-PMCID-DOI mappings into a dictionary."""
    mappings = {}

    try:
        with open(mapping_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if len(row) >= 3:
                    pmcid = row[1].strip()
                    doi = row[2].strip()
                    if pmcid and doi:
                        mappings[pmcid] = format_doi(doi)
    except Exception as e:
        print(f"Error loading DOI mappings: {e}")

    return mappings


def format_doi(doi):
    """Format DOI as a URL if it's a valid DOI."""
    return "https://doi.org/" + doi if doi.startswith('10.') else doi


def load_repository_mappings():
    """
    Load repository name mappings based on the "Repository title" column
    in the EuropePMC summary spreadsheet.
    """
    mapping_file_path = "repository_mapping.json"
    with open(mapping_file_path, 'r') as f:
        return json.load(f)


def process_csv_file(input_file, output_file, doi_mappings, repository_name, api_cache):
    """
    Process a single CSV file and create a reformatted version.
    Assumes all CSV files have dataset_id in first column and PMCID in second column.
    """
    try:
        output_data = []
        api_count = 0
        row_count = 0
        cache_hit_count = 0

        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)

            for row in reader:
                if len(row) < 2 or not row[0] or not row[1]:
                    continue

                row_count += 1
                dataset_id = row[0].strip()
                pmcid = row[1].strip()

                doi_url = doi_mappings.get(pmcid, '')

                if not doi_url and (pmcid in api_cache):
                    doi_url = api_cache[pmcid]
                    cache_hit_count += 1

                if not doi_url:
                    doi_url = get_doi_from_api(pmcid)
                    api_count += 1

                    time.sleep(0.1)

                    if doi_url:
                        api_cache[pmcid] = doi_url

                    if api_count % 450 == 0:
                        print(f"Made {api_count} API calls, pausing for 10 seconds...")
                        time.sleep(10)

                if doi_url:
                    if repository_name == 'doi':
                        repository_name = get_repository_from_api(dataset_id)
                    output_data.append({
                        'repository': repository_name,
                        'dataset': dataset_id,
                        'publication': doi_url
                    })

        output_df = pd.DataFrame(output_data)

        output_df.to_csv(output_file, index=False)
        success_rate = (len(output_df) / row_count) * 100 if row_count > 0 else 0
        print(f"Created formatted CSV: {output_file} with {len(output_df)}/{row_count} entries ({success_rate:.1f}%)")
        print(f"API calls: {api_count}, Cache hits: {cache_hit_count}")

    except Exception as e:
        print(f"Error processing {input_file}: {e}")


def get_doi_from_api(pmcid):
    """Fetch DOI information from EuropePMC API with rate limiting."""
    if not pmcid.startswith('PMC'):
        pmcid = f"PMC{pmcid}"

    api_url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/article/PMC/{pmcid}?resultType=lite&format=json"

    try:
        response = requests.get(api_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'result' in data and 'doi' in data['result']:
                return format_doi(data['result']['doi'])
    except Exception as e:
        print(f"API error for {pmcid}: {e}")

    return ""


def get_repository_from_api(doi):
    """Fetch repository (publisher) information from DataCite API."""
    if not doi or not doi.startswith('10.'):
        return None

    api_url = f"https://api.datacite.org/dois/{doi}"

    try:
        time.sleep(0.02)
        response = requests.get(api_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and 'attributes' in data['data']:
                return data['data']['attributes'].get('publisher', None)
        elif response.status_code == 429:
            print(f"Rate limit exceeded for DOI {doi}. Retrying after 60 seconds...")
            time.sleep(60)
            return get_repository_from_api(doi)  # Retry after waiting
        else:
            print(f"DataCite API returned status code {response.status_code} for DOI {doi}")

        return None
    except Exception as e:
        print(f"API error for DOI {doi}: {e}")
        return None


if __name__ == "__main__":
    main()

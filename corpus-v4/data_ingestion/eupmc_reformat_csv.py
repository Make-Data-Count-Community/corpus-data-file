#!/usr/bin/env python3

import os
import csv
import glob
import json
import requests
import time
from datetime import datetime
import pandas as pd
from urllib.parse import urlencode


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


def process_csv_file(input_file, output_file, doi_mappings, repository_name, api_cache):
    """
    Process a single CSV file and create a reformatted version.
    First collect missing PMCIDs, then batch fetch them, then process the file.
    """
    try:
        print(f"  Step 1: Analyzing {input_file} for missing PMCIDs...")
        missing_pmcids = collect_missing_pmcids_for_file(input_file, doi_mappings, api_cache)
        
        if missing_pmcids:
            print(f"  Step 2: Found {len(missing_pmcids)} missing PMCIDs, batch fetching DOIs...")
            new_dois = batch_fetch_dois(list(missing_pmcids))
            api_cache.update(new_dois)
            print(f"  Step 3: Fetched {len(new_dois)} new DOIs from API")
        else:
            print(f"  Step 2: All PMCIDs already have DOIs, skipping API calls")

        print(f"  Step 3: Processing file and creating output...")
        process_file_with_complete_data(input_file, output_file, doi_mappings, repository_name, api_cache)

    except Exception as e:
        print(f"Error processing {input_file}: {e}")


def collect_missing_pmcids_for_file(csv_file, doi_mappings, api_cache):
    """Collect PMCIDs from a specific file that don't have DOIs in mappings or cache."""
    missing_pmcids = set()
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            
            for row in reader:
                if len(row) < 2 or not row[0] or not row[1]:
                    continue
                    
                pmcid = row[1].strip()
                
                # Check if we already have DOI for this PMCID
                if pmcid not in doi_mappings and pmcid not in api_cache:
                    missing_pmcids.add(pmcid)
                    
    except Exception as e:
        print(f"Error collecting missing PMCIDs from {csv_file}: {e}")
    
    return missing_pmcids


def batch_fetch_dois(pmcids, batch_size=1000):
    """Fetch DOIs for multiple PMCIDs using the bulk search API."""
    doi_results = {}
    total_batches = (len(pmcids) + batch_size - 1) // batch_size
    
    for i in range(0, len(pmcids), batch_size):
        batch = pmcids[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        
        print(f"    Processing batch {batch_num}/{total_batches} ({len(batch)} PMCIDs)...")
        
        # Format PMCIDs for query (ensure they have PMC prefix)
        formatted_pmcids = []
        for pmcid in batch:
            if not pmcid.startswith('PMC'):
                formatted_pmcids.append(f'PMC{pmcid}')
            else:
                formatted_pmcids.append(pmcid)
        
        # Create query string: PMCID:(PMC123 OR PMC456 OR ...)
        query = f"PMCID:({' OR '.join(formatted_pmcids)})"
        
        try:
            response = fetch_batch_from_api(query)
            if response:
                # Extract DOIs from response
                batch_dois = extract_dois_from_response(response)
                doi_results.update(batch_dois)
                print(f"    Batch {batch_num}: Found DOIs for {len(batch_dois)} PMCIDs")
            
            # Rate limiting - wait between batches
            if batch_num < total_batches:
                time.sleep(1)  # 1 second between batches
                
        except Exception as e:
            print(f"    Error processing batch {batch_num}: {e}")
            time.sleep(5)  # Wait longer on error
    
    return doi_results


def fetch_batch_from_api(query):
    """Make a single batch request to the EuropePMC search API."""
    url = "https://www.ebi.ac.uk/europepmc/webservices/rest/searchPOST"
    
    data = {
        'query': query,
        'resultType': 'lite',
        'pageSize': 1000,
        'format': 'json'
    }
    
    try:
        response = requests.post(
            url,
            data=urlencode(data),
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"    API returned status code {response.status_code}")
            return None
            
    except Exception as e:
        print(f"    API request error: {e}")
        return None


def extract_dois_from_response(response_data):
    """Extract PMCID to DOI mappings from API response."""
    doi_mappings = {}
    
    try:
        if 'resultList' in response_data and 'result' in response_data['resultList']:
            results = response_data['resultList']['result']
            
            for result in results:
                pmcid = result.get('pmcid', '')
                doi = result.get('doi', '')
                
                if pmcid and doi:
                    doi_mappings[pmcid] = format_doi(doi)
                    
    except Exception as e:
        print(f"    Error extracting DOIs from response: {e}")
    
    return doi_mappings


def process_file_with_complete_data(input_file, output_file, doi_mappings, repository_name, api_cache):
    """Process the CSV file now that we have all the DOI data."""
    output_data = []
    row_count = 0
    found_dois = 0

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)  # Skip header

        for row in reader:
            if len(row) < 2 or not row[0] or not row[1]:
                continue

            row_count += 1
            dataset_id = row[0].strip()
            pmcid = row[1].strip()

            # Get DOI from mappings or cache
            doi_url = doi_mappings.get(pmcid, '') or api_cache.get(pmcid, '')

            if doi_url:
                found_dois += 1
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
    print(f"  Created formatted CSV: {output_file} with {len(output_df)}/{row_count} entries ({success_rate:.1f}%)")
    print(f"  Found DOIs for {found_dois}/{row_count} PMCIDs")


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

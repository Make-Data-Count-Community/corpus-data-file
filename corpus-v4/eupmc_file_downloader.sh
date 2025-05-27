#!/bin/bash

# Create directory
mkdir -p europepmc_raw_data
cd europepmc_raw_data

excluded_files=("cellosaurus.csv" "chebi.csv" "ebisc.csv" "eudract.csv" "hipsci.csv" "nct.csv" "omim.csv" "rrid.csv")

curl -s https://europepmc.org/pub/databases/pmc/TextMinedTerms/ | 
grep -o 'href="[^"]*\.csv"' | 
cut -d'"' -f2 | 
while read -r file; do
    skip=false
    for excluded in "${excluded_files[@]}"; do
        if [ "$file" == "$excluded" ]; then
            echo "Skipping excluded file $file..."
            skip=true
            break
        fi
    done
    
    if [ "$skip" == "false" ]; then
        echo "Downloading $file..."
        curl -O "https://europepmc.org/pub/databases/pmc/TextMinedTerms/$file"
    fi
done

echo "All files downloaded except excluded ones."

echo "Downloading DOI metadata file..."
curl -O https://europepmc.org/pub/databases/pmc/DOI/PMID_PMCID_DOI.csv.gz

echo "Unzipping DOI metadata file..."
gunzip PMID_PMCID_DOI.csv.gz

echo "Removing compressed file..."
rm PMID_PMCID_DOI.csv.gz

echo "Download complete!"

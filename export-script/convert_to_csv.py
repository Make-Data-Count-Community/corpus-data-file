import json
import csv
import sys
import os

def get_all_data(json_file, output_dir):
    base_filename = os.path.splitext(os.path.basename(json_file))[0]
    outfile = os.path.join(output_dir, f"{base_filename}.csv")

    with open(outfile, 'w', newline='', encoding='utf-8') as f_out:
      writer = csv.writer(f_out)
      writer.writerow(
        ['id', 'created', 'updated', 'repository', 'publisher', 'journal', 'title',
         'dataset', 'publication', 'publishedDate',
         'source', 'subjects', 'affiliations', 'funders'
        ]
      )
    with open(json_file, 'r', encoding='utf-8') as f_in:
      json_data = json.load(f_in)
    for record in json_data:
        # Basic details
        id = record['id']
        created = record['created']
        updated = record['updated']
        title = record['title']
        dataset = record['dataset']
        publication = record['publication']
        publishedDate = record.get('publishedDate', '')
        source = record.get('source', '')

        # Repository
        repository_data = record.get('repository', {})
        repository_title = repository_data.get('title','')
        repository_external_id = repository_data.get('external_id','')
        repository = f"{repository_title} {repository_external_id}" if repository_external_id else repository_title

        # Publisher
        publisher_data = record.get('publisher', {})
        publisher_title = publisher_data.get('title','')
        publisher_external_id = publisher_data.get('external_id','')
        publisher = f"{publisher_title} {publisher_external_id}" if publisher_external_id else publisher_title

        # Journal
        journal_data = record.get('journal', {})
        journal_title = journal_data.get('title','')
        journal_external_id = journal_data.get('external_id','')
        journal = f"{journal_title} {journal_external_id}" if journal_external_id else journal_title

        # Subjects
        subjects = '; '.join(record.get('subjects', []))

        # Affiliations
        affiliations = '; '.join([f"{aff['title']}{' ' + aff['external_id'] if aff.get('external_id') else ''}" for aff in record.get('affiliations', [])])

        # Funders
        funders = '; '.join([f"{funder['title']}{' ' + funder['external_id'] if funder.get('external_id') else ''}" for funder in record.get('funders', [])])

        with open(outfile, 'a', newline='', encoding='utf-8') as f_out:
            writer = csv.writer(f_out)
            writer.writerow([id, created, updated, repository, publisher, journal, title, dataset, publication, publishedDate, source, subjects, affiliations, funders])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python script.py input.json")
        sys.exit(1)
    get_all_data(sys.argv[1], sys.argv[2])

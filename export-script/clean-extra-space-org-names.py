import argparse
import os
import csv
import copy

OUTPUT_PATH = 'updated_csv/'

def remove_spaces(file):
    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)
    print("PROCESSING CSV")
    try:
        input_path, filename = os.path.split(file)
        with open(os.path.join(OUTPUT_PATH, filename), 'w', newline='', encoding='utf-8') as f_out:
            writer = csv.writer(f_out)
            writer.writerow(
                ['id', 'created', 'updated', 'repository', 'publisher', 'journal', 'title',
                'objId', 'subjId', 'publishedDate', 'accessionNumber', 'doi', 'relationTypeId',
                'source', 'subjects', 'affiliations', 'funders'
                ]
            )
        with open(file, 'r') as input:
            reader = csv.DictReader(input)
            #fieldnames = copy(list(reader)[0])

            for row in reader:
                if row['affiliations']:
                    row['affiliations'] = row['affiliations'].replace(' ;', ';')
                if row['affiliations']:
                    row['funders'] = row['funders'].replace(' ;', ';')
                with open(os.path.join(OUTPUT_PATH, filename), 'a', newline='', encoding='utf-8') as f_out:
                    writer = csv.writer(f_out)
                    writer.writerow([row['id'], row['created'], row['updated'], row['repository'], row['publisher'], row['journal'], row['title'],
                        row['objId'], row['subjId'], row['publishedDate'], row['accessionNumber'], row['doi'], row['relationTypeId'],
                        row['source'], row['subjects'], row['affiliations'], row['funders']
                        ]
                    )
    except IOError as e:
        print(f"Error reading file {file}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to remove extra spaces before ; in CSV fields")
    parser.add_argument('file')
    args = parser.parse_args()
    remove_spaces(args.file)
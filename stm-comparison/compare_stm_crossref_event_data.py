import csv
import requests
from datetime import datetime

EVENT_DATA_API_URL = 'https://api.eventdata.crossref.org/v1/events'

CITATION_REL_TYPES = ['is-referenced-by',
        'is-cited-by',
        'is-supplement-to',
        'references',
        'cites',
        'is-supplemented-by'
    ]

SUBJ = 'subj-id'
OBJ = 'obj-id'


def in_event_data(article_doi, dataset_doi):
    params = {SUBJ: article_doi, OBJ: dataset_doi}
    headers = {"mailto":"liz.krznarich@datacite.org"}
    response = requests.get(EVENT_DATA_API_URL, params=params, headers=headers)
    response_json = response.json()
    for event in response_json['message']['events']:
        if event['relation_type_id'] in CITATION_REL_TYPES:
            return True
    return False

def parse_csv(input_file):
    with open(input_file, 'r+') as f_in:
        output = []
        reader = csv.DictReader(f_in)
        for row in reader:
            article_doi = row['Article DOI']
            stm_cited_dois = row['Datacite References'].strip('[]').split(',')
            stm_cited_dois = [i.strip('\" ') for i in stm_cited_dois]
            stm_cited_dois  = [i for i in stm_cited_dois if i != '']
            not_in_event_data = []
            for stm_cited_doi in stm_cited_dois:
                is_in_event_data = in_event_data(article_doi, stm_cited_doi)
                if not is_in_event_data:
                    not_in_event_data.append(stm_cited_doi)
            output.append([article_doi, ', '.join(stm_cited_dois), len(stm_cited_dois), ', '.join(not_in_event_data), len(not_in_event_data), len(stm_cited_dois)-len(not_in_event_data)])
            #print(f"{article_doi}|{','.join(stm_cited_dois)}|{len(stm_cited_dois)}|{','.join(datacite_cited_dois)}|{len(datacite_cited_dois)}|{str(stm_same_as_datacite)}|{len(stm_cited_dois)-len(datacite_cited_dois)}")
        return output

def write_to_csv(report_data):
    today = datetime.today().strftime('%Y-%m-%d')
    with open(today + '-stm_crossref_event_data_comparison.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['Article DOI', 'STM cited DOIs', 'Count', 'Not in Crossref Event Data', 'Count', 'Count in Crossref Event Data'])
        writer.writerows(report_data)

def main():
    print(f"Parsing CSV")
    report_data = parse_csv('doi_references_agu_2023_09.csv')
    print(f"Writing report file")
    write_to_csv(report_data)


if __name__ == '__main__':
    main()
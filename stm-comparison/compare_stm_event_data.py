import csv
import requests
from datetime import datetime

EVENT_DATA_API_URL = 'https://api.datacite.org/events'

CITATION_REL_TYPES = ['is-referenced-by',
        'is-cited-by',
        'is-supplement-to',
        'references',
        'cites',
        'is-supplemented-by'
    ]

SUBJ = 'subj_id'
OBJ = 'obj_id'

def check_if_dois_same(stm_cited_dois, datacite_cited_dois):
    if len(stm_cited_dois) != len(datacite_cited_dois):
        return False
    else:
        stm_not_in_datacite = [i for i in stm_cited_dois if i not in datacite_cited_dois]
        datacite_not_in_stm = [i for i in datacite_cited_dois if i not in stm_cited_dois]
        if stm_not_in_datacite or datacite_not_in_stm:
            return False
        return True

def get_citation_events(article_doi, filter_param):
    cited_dois = []
    params = {filter_param: article_doi}
    response = requests.get(EVENT_DATA_API_URL, params=params)
    response_json = response.json()
    for event in response_json['data']:
        if event['attributes']['relation-type-id'] in CITATION_REL_TYPES:
            if event['attributes']['obj-id'] not in cited_dois:
                cited_dois.append(event['attributes']['obj-id'])
    return cited_dois

def parse_csv(input_file):
    with open(input_file, 'r+') as f_in:
        output = []
        reader = csv.DictReader(f_in)
        for row in reader:
            article_doi = row['Article DOI']
            stm_cited_dois = row['Datacite References'].strip('[]').split(',')
            stm_cited_dois = [i.strip('\" ') for i in stm_cited_dois]
            datacite_cited_dois_subj = get_citation_events(article_doi, SUBJ)
            datacite_cited_dois_obj = get_citation_events(article_doi, OBJ)
            datacite_cited_dois = datacite_cited_dois_subj + [i for i in datacite_cited_dois_obj if i not in datacite_cited_dois_subj]
            stm_same_as_datacite = check_if_dois_same(stm_cited_dois, datacite_cited_dois)
            stm_dois_not_in_datacite = [i for i in stm_cited_dois if i not in datacite_cited_dois]
            datacite_dois_not_in_stm = [i for i in datacite_cited_dois if i not in stm_cited_dois]
            output.append([article_doi, stm_cited_dois, len(stm_cited_dois), datacite_cited_dois, len(datacite_cited_dois), str(stm_same_as_datacite), len(stm_cited_dois)-len(datacite_cited_dois)])
            #print(f"{article_doi}|{','.join(stm_cited_dois)}|{len(stm_cited_dois)}|{','.join(datacite_cited_dois)}|{len(datacite_cited_dois)}|{str(stm_same_as_datacite)}|{len(stm_cited_dois)-len(datacite_cited_dois)}")

def write_to_csv(report_data):
    today = datetime.today().strftime('%Y-%m-%d')
    with open(today + 'stm_event_data_comparison.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['Article DOI', 'STM cited DOIs', 'Count', 'DataCite Event Data cited DOIs', 'Count', 'STM results same as DataCite Event Date', 'Difference in STM vs DataCite cited DOIs count'])
        writer.writerows(report_data)

def main():
    print(f"Parsing CSV")
    report_data = parse_csv('doi_references_agu_2023_09.csv')
    print(f"Writing report file")
    write_to_csv(report_data)


if __name__ == '__main__':
    main()
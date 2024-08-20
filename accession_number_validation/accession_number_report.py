import os
import argparse
import pandas as pd
import csv


def get_summary(csv_file):
    df = pd.read_csv(csv_file)
    return [os.path.split(csv_file)[1], df['repository_title'].loc[df.index[1]], len(df.index), df['accession_number_pattern_match'].value_counts()[False], format(df['accession_number_pattern_match'].value_counts()[False]/len(df.index), ".0%")]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate report from Data Citation Corpus accession number comparison CSV files")
    parser.add_argument("-d", "--dir", help="Path to a directory that contains accession number comparison CSV files")
    return parser.parse_args()


def main():
    args = parse_args()
    if os.path.exists(args.dir):
        csv_files = os.listdir(args.dir)
        report = []
        for csv_file in csv_files:
            report.append(get_summary(os.path.join(args.dir,csv_file)))
        with open('report.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["file", "repository_title", "total_accession_numbers", "false_match", "percent_false_match"])
            for row in report:
                writer.writerow(row)
    else:
         print(f"Directoy {dir} does not exist")


if __name__ == "__main__":
	main()



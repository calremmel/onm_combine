import csv
import datetime
import os
import subprocess
from collections import Counter

from rich import print
from tqdm import tqdm


def get_file_length(path):
    length = int(subprocess.check_output(f"wc -l {path}".split()).split()[0].decode())
    return length


def fix_which_test(row):
    tested_last30_bool = row["tested_for_covid19_last30d"] == "1"
    sought_testing_bool = row["seen_health_professional_sought_testing"] == "1"
    test_condition_bool = tested_last30_bool or sought_testing_bool
    if not test_condition_bool:
        return row
    for k in row.keys():
        if "what_type_medical" in k:
            if row[k] != "1":
                row[k] = "0"
    return row


def fix_symptom(row):
    sick = row["how_are_you_feeling"] == "2"
    if not sick:
        return row
    for k in row.keys():
        if "symptom_last7_day_mc_" in k:
            if row[k] != "1":
                row[k] = "0"
    return row


def get_fields(full_data, adhoc_files, weights_column):
    fields = []
    with open(full_data) as f:
        data_reader = csv.DictReader(f)
        temp_fields = list(data_reader.fieldnames)
        for field in temp_fields:
            fields.append(field.lower())
    for af in adhoc_files:
        with open(af) as g:
            data_reader = csv.DictReader(g)
            temp_fields = list(data_reader.fieldnames)
            for field in temp_fields:
                if field.lower() not in fields:
                    fields.append(field.lower())
    if weights_column not in fields:
        fields.append(weights_column)
    fields = [field for field in fields if field not in ['q73', 'q74']]
    print(sorted(fields))
    return fields


def combine_data(full_data, full_weights, adhoc_files, outfile):
    weights_column = "weight_daily_national_13plus"
    fields = get_fields(full_data, adhoc_files, weights_column)
    full_data_rows = get_file_length(full_data) - 1
    full_weights_rows = get_file_length(full_weights) - 1
    if not full_data_rows == full_weights_rows:
        raise ValueError("Data and weights must be the same length")
    adhoc_files_rows = [get_file_length(af) - 1 for af in adhoc_files]

    print()
    with open(full_data) as f, open(full_weights) as g:
        print("Reading last full data and weights files...")
        data_reader = csv.DictReader(f)
        weights_reader = csv.DictReader(g)
        print("Files read and fields loaded!")
        with open(outfile, "w") as combined:
            writer = csv.DictWriter(
                combined, fields, restval="NA", extrasaction="ignore"
            )
            writer.writeheader()
            print("Combining and writing full data and full weights...")
            with tqdm(total=full_data_rows, desc="Base rows written: ") as bar:
                for d_row, w_row in zip(data_reader, weights_reader):
                    temp_row = d_row
                    temp_row[weights_column] = w_row[weights_column]
                    writer.writerow(temp_row)
                    bar.update()
            print("Full data and weights combined!")
            print("Loading and writing ad-hoc files...")
            for af, ad_len in zip(adhoc_files, adhoc_files_rows):
                with open(af) as h:
                    adhoc_reader = csv.DictReader(h)
                    with tqdm(total=ad_len, desc="Adhoc rows written: ") as bar:
                        for row in adhoc_reader:
                            start_date = row["start_time"][0 : len("YYYY-MM-DD")]
                            row["start_date"] = start_date
                            row = fix_which_test(row)
                            row = fix_symptom(row)
                            writer.writerow(row)
                            bar.update()
    print("Done!")
    print(outfile)
    return outfile


def get_adhoc_files():
    prefix = "../data/"
    data_files = os.listdir(prefix)
    files = sorted([prefix + x for x in data_files if x.endswith("onm-adhoc.csv")])
    return files


def main():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S")
    outfile = f"{timestamp}_onm-combined.csv"
    files = get_adhoc_files()
    combine_data(
        "../data/2022-05-22_full-data.csv",
        "../data/2022-05-22_full-data_weights.csv",
        files,
        outfile,
    )


if __name__ == "__main__":
    main()

import os
import csv

from element_pairs.elements_index import ElementsIndex
from samples_index.samples_index import SamplesIndex
from element_pairs.element_pair import ElementPair

from itertools import combinations


def pair_folder_name(pair: tuple):
    return f"{pair[0]}_{pair[1]}"


def pair_folder_path(pair: tuple):
    name = pair_folder_name(pair)
    path = os.path.join("pairs_data", name)
    return path


def result_to_csv_row(result: tuple) -> list:
    row = []
    sample_id = result[0]
    row.append(sample_id.decode('ascii'))
    results = result[1]
    for r in results:
        row.extend(r)
    return row


def result_valid(result: tuple) -> bool:
    results = result[1]
    if len(results) != 2:
        return False
    return True


def write_pair_csv(pair: tuple, results: list):
    dirpath = pair_folder_path(pair)
    invalid_count = 0
    valid_count = 0
    print(len(results))
    results_path = os.path.join(dirpath, 'sample_results.csv')
    with open(results_path, 'wt') as csv_out:
        wr = csv.writer(csv_out)
        for r in results:
            if result_valid(r):
                row = result_to_csv_row(r)
                wr.writerow(row)
                valid_count += 1
            else:
                invalid_count += 1
    print(f"Wrote {valid_count} samples - {invalid_count} invalid")

    completed = os.path.join(dirpath, '_created_ok_')
    open(completed, 'a').close()


def create_pair_folder(pair: tuple):
    dirpath = pair_folder_path(pair)
    if not os.path.isdir(dirpath):
        os.mkdir(dirpath)


def pair_folder_processed_ok(pair: tuple):

    dirpath = pair_folder_path(pair)
    if os.path.isdir(dirpath):
        completed_file = os.path.join(dirpath, '_created_ok_')
        return os.path.isfile(completed_file)
    else:
        return False


if __name__ == '__main__':

    samples_index = SamplesIndex()
    elements_index = ElementsIndex()

    all_els = samples_index.all_elements
    els_present = list(filter(lambda x: x in elements_index.elements_by_symbol, all_els))
    print(f"All symbols count is {len(all_els)}, {len(els_present)} are valid.")
    print(samples_index.all_elements)
    print(els_present)

    elements_pairs = list(combinations(els_present, 2))
    combs_count = len(elements_pairs)

    for idx, t in enumerate(elements_pairs):

        print("Output folder:", pair_folder_name(t))

        if pair_folder_processed_ok(t):
            print("Skip, already present")
            continue

        create_pair_folder(t)

        # print(t)
        et = ElementPair(t, elements_index, samples_index)

        results = et.sample_results()
        print(f"{idx:7d} / {combs_count} {str(et.elements):16s} {len(results)} samples")

        write_pair_csv(t, results)


import os
import csv

from element_triples.elements_index import ElementsIndex
from samples_index.samples_index import SamplesIndex
from element_triples.element_triple import ElementTriple
from itertools import combinations


def triple_folder_name(triple: tuple):
    return f"{triple[0]}_{triple[1]}_{triple[2]}"


def triple_folder_path(triple: tuple):
    name = triple_folder_name(triple)
    path = os.path.join("triples_data", name)
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
    if len(results) != 3:
        return False
    return True


def write_triple_csv(triple: tuple, results: list):
    dirpath = triple_folder_path(triple)
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


def create_triple_folder(triple: tuple):
    dirpath = triple_folder_path(triple)
    if not os.path.isdir(dirpath):
        os.mkdir(dirpath)


def triple_folder_processed_ok(triple: tuple):

    dirpath = triple_folder_path(triple)
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

    elements_triples = list(combinations(els_present, 3))
    combs_count = len(elements_triples)

    for idx, t in enumerate(elements_triples):

        print("Output folder:", triple_folder_name(t))

        if triple_folder_processed_ok(t):
            print("Skip, already present")
            continue

        create_triple_folder(t)

        # print(t)
        et = ElementTriple(t, elements_index, samples_index)

        results = et.sample_results()
        print(f"{idx:7d} / {combs_count} {str(et.elements):16s} {len(results)} samples")

        write_triple_csv(t, results)


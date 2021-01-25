import csv
import time
import json

from collections import defaultdict


def _load_csv_to_list(filename: str) -> list:

    rows = []
    with open(filename) as csvfile:
        rdr = csv.reader(csvfile)
        next(rdr)
        for rw in rdr:
            rows.append(rw)
    return rows


# RPT_ID,HOLEID,SAMPLEID,FILE_ID,SMP_ID,TOPP,BASE,SAMPCODE,UPDATED
def load_dgc_sample():
    print("Loading dgc_samples")
    dgc_samples = _load_csv_to_list("../data/GSNSWDataset/DD_DGC_SAMPLE.csv")
    print(f"Samples count = {len(dgc_samples)}")
    return dgc_samples


# FILE_ID,SMP_ID,ASS_ID,RESULT
def load_dgc_data():
    print("Loading dgc_data")
    dgc_data = _load_csv_to_list("../data/GSNSWDataset/DD_DGC_DATA.csv")
    print(f"Data count = {len(dgc_data)}")
    return dgc_data


# FILE_ID,ASS_ID,ELEMENT,ASSAYCODE,UNITS,DETLIMIT,ACCURACY,UPDETLIMIT
def load_gch_meta():
    print("Loading gch_meta")
    gch_meta = _load_csv_to_list("../data/GSNSWDataset/DD_GCH_META.csv")
    print(f"Meta count = {len(gch_meta)}")
    return gch_meta


def build_samples(samples_rows, data_rows, meta_row):

    data_indexed = defaultdict(list)
    meta_indexed = {}

    samples = {}

    # index data by (file_id, smp_id)
    for d in data_rows:
        file_id = d[0]
        smp_id = d[1]
        key = (file_id, smp_id)
        data_indexed[key].append(d)

    # index meta by (file_id, ass_id)
    for m in meta_row:
        file_id = m[0]
        ass_id = m[1]
        key = (file_id, ass_id)
        meta_indexed[key] = m

    data_points_count = 0

    for s in samples_rows:   # [:100]
        # print()
        # print("sample  |", s)
        file_id = s[3]
        hole_id = s[1]
        smp_id = s[4]
        sample_id = s[2]
        rpt_id = s[0]
        key = (file_id, smp_id)

        results = []
        sample = {
            "rpt_id": rpt_id,
            "file_id": file_id,
            "smp_id": smp_id,
            "sample_id": sample_id,
            "hole_id": hole_id,
            "results": results
        }

        sample_key = f"{rpt_id}.{hole_id}.{sample_id}"  # .{file_id}.{smp_id}
        assert sample_key not in samples
        samples[sample_key] = sample

        data_results_for_sample = data_indexed[key]
        # print(data_results_for_sample)
        data_points_count += len(data_results_for_sample)
        assert isinstance(data_results_for_sample, list)
        for res in data_results_for_sample:
            # print("  result  |", res)
            ass_id = res[2]
            meta_key = (file_id, ass_id)
            meta_row = meta_indexed[meta_key]
            # print("  meta    |", meta_rows)
            result = {
                "element": meta_row[2],
                "assay": meta_row[3],
                "unit": meta_row[4],
                "value": res[3]
            }
            results.append(result)

    print("data points = ", data_points_count)

    return samples


def load_dgc():

    t0 = time.time()
    samples = load_dgc_sample()
    data = load_dgc_data()
    meta = load_gch_meta()

    t1 = time.time()

    print(f"Datasets loaded in {(t1-t0):2f}")
    samples = build_samples(samples, data, meta)

    # print(samples)

    return samples


if __name__ == '__main__':
    samples = load_dgc()

    with open("../data/drill_samples.json", 'wt') as json_out:
        json.dump(samples, json_out, indent=4)






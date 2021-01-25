import json
import h5py
import time

t0 = time.time()
with open("../data/drill_samples.json") as samples_json:
    samples = json.load(samples_json)
t1 = time.time()

print(f"Data loaded in {(t1 - t0):2f}")

samples_count = len(samples)
results_count = 0
for key, s in samples.items():
    sample_results = s['results']
    results_count += len(sample_results)

print("samples:", samples_count)
print("results:", results_count)


def max_len(strings_list):
    print(len(strings_list))
    max_found = 0
    for s in strings_list:
        if (l := len(s)) > max_found:
            max_found = l
    return max_found


with h5py.File("../data/drill_samples.003.hdf5", "w") as samples_h5:

    samples_group = samples_h5.create_group('drill_samples')

    # string_dt = h5py.string_dtype(encoding='utf-8')

    # write a dataset of ids first
    sample_ids = list(samples.keys())
    dtype = 'S33'
    # print(f"max length found in sample_ids = {max_len(sample_ids)} vs {dtype}")
    samples_group.create_dataset('drill_sample_id', (samples_count,), dtype=dtype, data=sample_ids, compression='gzip')


    # write a dataset for each sample field
    for field, dtype in (
        ("rpt_id", 'i'),
        ("hole_id", 'S10'),
        ("sample_id", 'S18'),
        ("file_id", 'i'),
        ("smp_id", 'i')
    ):
        print(field, dtype)
        values = []
        for key, s in samples.items():
            values.append(s[field])

        print(len(values))
        if dtype[0] == 'S':
            # samples_h5.create_dataset(f'drill_samples/{field}', (samples_count,), dtype='S20', data=values)
            # print(f"max length found in {field} = {max_len(values)} vs {dtype}")
            samples_group.create_dataset(f'{field}', (samples_count,), dtype=dtype, data=values, compression='gzip')
            pass
        elif dtype == 'i':
            # samples_h5.create_dataset(f'drill_samples/{field}', (samples_count,), dtype='i', data=values)
            samples_group.create_dataset(f'{field}', (samples_count,), dtype=h5py.h5t.NATIVE_INT32, data=values, compression='gzip')

    for name in samples_h5:
        print(name)

    # extract results from samples

    results_group = samples_h5.create_group('drill_sample_results')

    sample_ids = []
    for key, s in samples.items():
        sample_results = s['results']
        for r in sample_results:
            sample_ids.append(key)
    print("SampleIDs in results group:", len(sample_ids))
    dtype = 'S33'
    results_group.create_dataset(f'drill_sample_id', dtype=dtype, data=sample_ids, compression='gzip')

    for field, dtype in (
            ("element", 'S8'),
            ("assay", 'S10'),
            ("unit", 'S10'),
            ("value", 'f')
    ):
        values = []
        for key, s in samples.items():
            sample_results = s['results']
            for r in sample_results:
                values.append(r[field])
        if dtype[0] == 'S':
            # print(f"max length found in {field} = {max_len(values)} vs {dtype}")
            # samples_h5.create_dataset(f'drill_samples/{field}', (samples_count,), dtype='S20', data=values)
            results_group.create_dataset(f'{field}', dtype=dtype, data=values, compression='gzip')
        elif dtype == 'f':
            # samples_h5.create_dataset(f'drill_samples/{field}', (samples_count,), dtype='i', data=values)
            results_group.create_dataset(f'{field}', dtype='float', data=values, compression='gzip')


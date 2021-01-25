import h5py
import numpy as np
import time


def load(path: str) -> dict:

    vf = np.vectorize(lambda x: x.decode('ascii'))

    extracted = {}

    t0 = time.time()

    with h5py.File(path, "r") as samples_h5:

        for name in samples_h5:
            # print(name)
            group = samples_h5[name]
            for dset_name in group:
                dset = group[dset_name]
                print(f"{name:24s} | {dset_name:20s} | {dset.dtype.char}")

                # Too slow...
                # if dset.dtype.char == 'S':
                #     # data = list(map(lambda x: x.decode('ascii'), data))
                #     print(" - convert bytes to str")
                #     data = dset.asstr()[()]
                # else:
                #     data = dset[()]

                data = dset[()]

                #print(type(data))
                assert isinstance(data, np.ndarray)
                extracted[f"{name}/{dset_name}"] = data

    t1 = time.time()
    print(f"Data loaded in {(t1 - t0):2f}")

    return extracted


if __name__ == '__main__':

    dsets = load("data/drill_samples.003.hdf5")
    print(len(dsets))
    for k, values in dsets.items():
        print()
        print(k, len(values), type(values))
        print(values[:5])

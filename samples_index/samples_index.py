import time
from load_h5 import load
from collections import defaultdict


class SamplesIndex(object):

    def __init__(self):
        self.elements_for_sample = defaultdict(set)
        self.sample_ids = None
        self.sample_results_by_sample_id = None

        self.sample_results_sample_ids = None
        self.sample_results_units = None
        self.sample_results_values = None
        self.sample_results_elements = None

        self.all_elements = None

        t0 = time.time()
        self._build()
        t1 = time.time()
        print(f"Indexing completed in {(t1 - t0):2f} s")

    def _build(self):

        dsets = load("../data/drill_samples.002.hdf5")
        # print(dsets.keys())

        self.sample_ids = dsets['drill_samples/drill_sample_id']
        self.sample_results_sample_ids = dsets['drill_sample_results/drill_sample_id']
        self.sample_results_units = dsets['drill_sample_results/unit']
        self.sample_results_values = dsets['drill_sample_results/value']
        self.sample_results_elements = dsets['drill_sample_results/element']

        self._build_elements_index(dsets)
        self._build_results_index()


    def _build_results_index(self):

        print("Indexing results")
        index = defaultdict(list)
        self.sample_results_by_sample_id = index
        for idx, result_sample_id in enumerate(self.sample_results_sample_ids):
            unit = self.sample_results_units[idx]
            unit = unit.decode('ascii')
            value = self.sample_results_values[idx]
            element = self.sample_results_elements[idx]
            element = element.decode('ascii')
            index[result_sample_id].append((unit, value, element))

    def _build_elements_index(self, dsets: dict):

        print("Indexing sample elements")
        elements_for_sample = self.elements_for_sample
        all_elements = set()

        results_sample_ids = dsets['drill_sample_results/drill_sample_id']
        results_elements = dsets['drill_sample_results/element']
        for idx, element in enumerate(results_elements):
            samp_id = results_sample_ids[idx]
            samp_elements_set = elements_for_sample[samp_id]
            element = element.decode('ascii')
            samp_elements_set.add(element)
            all_elements.add(element)

        self.all_elements = all_elements

    def samples_with_elements(self, elements: set):

        print("Search for samples with", elements )

        t0 = time.time()

        samp_ids = []
        elements_for_sample = self.elements_for_sample
        for idx, samp_id in enumerate(self.sample_ids):
            samp_els = elements_for_sample[samp_id]
            # print(samp_els)
            if elements.issubset(samp_els):
                samp_ids.append(samp_id)

        t1 = time.time()
        print(f"Search complete in {(t1 - t0):2f} s")

        return samp_ids

    def sample_results_for_sample(self, drill_sample_id: str):

        results = self.sample_results_by_sample_id[drill_sample_id]
        return results

    def sample_results_for_elements(self, elements: set):

        samp_ids = self.samples_with_elements(elements)

        results_for_els = []

        for sid in samp_ids:
            results = self.sample_results_for_sample(sid)

            sample_results = []
            for r in results:
                el = r[2]
                if el in elements:
                    # print(f"[{sid}] {r}")
                    sample_results.append(r)
            results_for_els.append((sid, sample_results))

        return results_for_els


if __name__ == '__main__':

    print("Load index")
    idx = SamplesIndex()

    els_set = set(('Au', 'As', 'Cu'))
    samp_ids = idx.samples_with_elements(els_set)
    print(len(samp_ids))

    els_set = set(('Pb', 'Zn', 'Ag'))
    samp_ids = idx.samples_with_elements(els_set)
    print(len(samp_ids))

    t0 = time.time()
    els_set = set(('Sm', 'Co', 'Nd'))
    samp_ids = idx.samples_with_elements(els_set)
    print(len(samp_ids))
    t1 = time.time()
    print(f"Elements matched in {(t1 - t0):2f} s")

    t0 = time.time()
    els_set = set(('Sm', 'Co', 'Nd'))
    samp_results = idx.sample_results_for_elements(els_set)
    print(len(samp_results))
    t1 = time.time()
    print(f"Element results found in {(t1 - t0):2f} s")

    for r in samp_results[:30]:
        print(r)

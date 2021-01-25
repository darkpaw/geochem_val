from element_triples.elements_index import ElementsIndex
from samples_index.samples_index import SamplesIndex


class ElementTriple(object):

    def __init__(self, els: tuple, elements_index: ElementsIndex, samples_index: SamplesIndex):

        self.elements_index = elements_index
        self.samples_index = samples_index

        assert len(els) == 3
        for el in els:
            assert el in self.elements_index.elements_by_symbol

        els = sorted(els, key=lambda x: self.elements_index.atomic_number(x))
        self.elements = els

    def sample_results(self):

        sample_results = self.samples_index.sample_results_for_elements(set(self.elements))
        return sample_results


if __name__ == '__main__':

    samples = SamplesIndex()
    elements = ElementsIndex()

    et = ElementTriple(('Sm', 'Co', 'Nd'), elements, samples)
    print(et.elements)

    results = et.sample_results()
    print(results[:5])


from load_csv.load_elements import load_elements


class ElementsIndex(object):

    def __init__(self):

        self.elements_by_symbol = {}
        elements = load_elements()
        # print(elements)
        for e in elements:
            self.elements_by_symbol[e[2]] = e

    def atomic_number(self, el: str):

        el = self.elements_by_symbol[el]
        return el[0]

    def name(self, el: str):

        el = self.elements_by_symbol[el]
        return el[1]


if __name__ == '__main__':

    ei = ElementsIndex()

    print(ei.atomic_number('Au'))
    print(ei.atomic_number('H'))





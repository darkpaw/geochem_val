import csv


def load_elements():

    elements = []

    with open("../data/elements.csv") as elcsv:

        rdr = csv.reader(elcsv)
        next(rdr)
        for r in rdr:
            num = r[0]
            el = r[1]
            sym = r[2]

            elements.append((num, el, sym))

    return elements

#!/usr/bin/python

import argparse
import os
from geoserver.catalog import Catalog


def main():

    parser = argparse.ArgumentParser(description="OWS12 Landsat script. Granules harvest tool.")
    parser.add_argument('-c', '--catalog', default=('http://localhost:8080/geoserver/rest',
                                                    'admin', 'geoserver'),
                        nargs=3, help='Geoserver REST URL and authentication')
    parser.add_argument('-s', '--store', nargs=1, help='The store name to retrieve coverages with workspace name (ows12:landsat)')
    parser.add_argument('granules', help='Path to ingestion file.')

    args = parser.parse_args()

    catalog = Catalog(service_url=args.catalog[0], username=args.catalog[1], password=args.catalog[2])
    store = catalog.get_store(args.store[0].split(':')[1], args.store[0].split(':')[0])

    with open(os.path.join(args.granules, 'download_files.txt'), 'r') as file:
        for l in file:
            # TODO: Enable parallel processing
            g = 'file://' + l.replace('\n', '')
            catalog.harvest_externalgranule(g, store)

if __name__ == '__main__':
    main()
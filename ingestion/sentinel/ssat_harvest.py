#!/usr/bin/python

import argparse
import os
import sys

from geoserver.catalog import Catalog
from ingestion.utils.utils import initLogger
from ingestion.scripts.sentinel import SentinelSat

# Make logger global here
logger = initLogger()


def main():

    parser = argparse.ArgumentParser(description="OWS12 Sentinel harvest granules script.")
    parser.add_argument('-c', '--catalog', nargs=3, help='Geoserver REST URL and authentication',
                        default=('http://localhost:8080/geoserver/rest', 'admin', 'geoserver'))
    parser.add_argument('-s', '--store', nargs=1, help='The store name to retrieve coverages with workspace name '
                                                       '(ows12:landsat)')
    parser.add_argument('-i', '--insert', action="store_true", default=False,
                        help='Harvest (insert) new granule on a given mosaic store')
    parser.add_argument('-d', '--delete', nargs=1, type=int, help='Delete granules older than given months value.')
    parser.add_argument('granules', help='Path to ingestion file.')

    args = parser.parse_args()

    sentinel = SentinelSat()

    # Before we proceed, check if we retrieved any files during previous processing step
    if not os.path.exists(os.path.join(args.granules, sentinel.processed_granules)):
        logger.info('Missing file from previous download job.\nPlease run this job first!')
        sys.exit(1)
    num_lines = sum(1 for l in open(os.path.join(args.granules, sentinel.processed_granules)))
    if num_lines == 0:
        logger.info('Skipping processing step. No files to process found!')
        sys.exit(0)

    catalog = Catalog(service_url=args.catalog[0], username=args.catalog[1], password=args.catalog[2])
    store = catalog.get_store(args.store[0].split(':')[1], args.store[0].split(':')[0])
    coverages = catalog.mosaic_coverages(store)

    # Harvest granules
    if args.insert:
        sentinel.insert_granules(args.granules, catalog, coverages, store)

    # Delete granules
    if args.delete:
        sentinel.delete_granules(catalog, coverages, store, args.delete)

if __name__ == '__main__':
    main()
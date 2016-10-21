#!/usr/bin/python

import argparse
import os
import sys
import time
from geoserver.catalog import Catalog
from utils.utils import initLogger

# Make logger global here
logger = initLogger()


def main():

    parser = argparse.ArgumentParser(description="OWS12 Landsat script. Granules harvest and delete tool.")
    parser.add_argument('-c', '--catalog', nargs=3, help='Geoserver REST URL and authentication',
                        default=('http://localhost:8080/geoserver/rest', 'admin', 'geoserver'))
    parser.add_argument('-s', '--store', nargs=1, help='The store name to retrieve coverages with workspace name '
                                                       '(ows12:landsat)')
    parser.add_argument('-i', '--insert', action="store_true", default=False,
                        help='Harvest (insert) new granule on a given mosaic store')
    parser.add_argument('-d', '--delete', nargs=1, type=int, help='Delete granules older than given months value.')
    parser.add_argument('granules', help='Path to ingestion file.')

    args = parser.parse_args()

    # Before we proceed, check if we retrieved any files during previous download step
    num_lines = sum(1 for l in open(os.path.join(args.granules, 'download_files.txt')))
    if num_lines == 0:
        logger.info('Skipping harvesting step. No files found!')
        sys.exit(0)

    catalog = Catalog(service_url=args.catalog[0], username=args.catalog[1], password=args.catalog[2])
    store = catalog.get_store(args.store[0].split(':')[1], args.store[0].split(':')[0])
    coverages = catalog.mosaic_coverages(store)

    # Harvest granules
    if args.insert:
        if not os.path.exists(os.path.join(args.granules, 'download_files.txt')):
            logger.info('Missing file from previous download job.\nPlease run this job first!')
            sys.exit(1)
        with open(os.path.join(args.granules, 'download_files.txt'), 'r') as file:
            for l in file:
                ingest = True
                gf = l.replace('\n', '').split('/')[-1]
                for c in coverages['coverages']['coverage']:
                    granules = catalog.mosaic_granules(c['name'], store, filter='location like \'%' + gf + '%\'')
                    if len(granules['features']) > 0:
                        logger.info('File ' + gf + ' already exists on database')
                        ingest = False
                if ingest:
                    logger.info('Ingesting granule ' + gf)
                    g = 'file://' + l.replace('\n', '')
                    catalog.harvest_externalgranule(g, store)

    # Delete granules
    if args.delete:
        today = time.strftime('%Y-%m-%dT00:00:00')
        for c in coverages['coverages']['coverage']:
            granules = catalog.mosaic_granules(c['name'], store, filter='time before P' + str(args.delete[0]) + 'M/' + today)
            if len(granules['features']) > 0:
                for g in granules['features']:
                    logger.info('Deleting granule ' + g['properties']['location'])
                    catalog.mosaic_delete_granule(c['name'], store, g['id'])
            else:
                logger.info('No granules to delete!')

if __name__ == '__main__':
    main()
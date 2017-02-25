#!/usr/bin/python

import argparse
import json
import os
import sys
from collections import defaultdict

from geoserver.catalog import Catalog
from ingestion.utils.db import DB
from ingestion.utils.utils import initLogger

from ingestion.scripts.lsat8 import Landsat

# Make logger global here
logger = initLogger()

def main():

    parser = argparse.ArgumentParser(description="OWS12 Landsat script. Wrapper to landsat-util scripts.")
    parser.add_argument('-l', '--limit', default=100, type=int,
                               help='Search return results limit\n'
                                    'default is 10')
    parser.add_argument('-s', '--start',
                               help='Start Date - Most formats are accepted\n'
                                    'e.g. Jun 12 2014 OR 06/12/2014')
    parser.add_argument('-e', '--end',
                               help='End Date - Most formats are accepted\n'
                                    'e.g. Jun 12 2014 OR 06/12/2014')
    parser.add_argument('-c', '--cloud', type=float, default=10.0,
                               help='Maximum cloud percentage\n'
                                    'default is 10 perct')
    parser.add_argument('-p', '--pathrow',
                               help='Paths and Rows in order separated by comma. Use quotes ("001").\n'
                                    'Example: path,row,path,row 001,001,190,204')
    parser.add_argument('--lat', type=float, help='The latitude')
    parser.add_argument('--lon', type=float, help='The longitude')
    parser.add_argument('--address', type=str, help='The address')

    # TODO: Add WKT support
    parser.add_argument("-w", "--wkt", nargs=1, metavar='WKT geometry',
                        help='Add a WKT geometry string in EPSG:4326 to add pathrows list to search files from.')

    # Download specific
    parser.add_argument('-b', '--bands', help='If you specify bands, landsat-util will try to download '
                                              'the band from S3. If the band does not exist, an error is returned\n'
                                              'Ex: 4 3 2', default='4 3 2', type=int, nargs='+')
    parser.add_argument('output', help='Destination path to save images')

    # Retrieve granules from gsconfig
    parser.add_argument('--catalog', nargs=3, help='Geoserver REST URL and authentication',
                        default=('http://localhost:8080/geoserver/rest', 'admin', 'geoserver'))
    parser.add_argument('--store', nargs=1, help='The store name to retrieve coverages with workspace name '
                                                 '(ows12:landsat)')

    parser.add_argument('--database', nargs=5, metavar=('db_name', 'db_host', 'db_port', 'db_user', 'db_password'),
                        help='Database connection params in the following order\n'
                             'db_name db_host db_port db_user db_password')

    args = parser.parse_args()

    # Append ingestion file
    file = open(os.path.join(args.output, 'ingest.txt'), 'w')
    # Search Scenes
    scenesgjson = Landsat().search(args)

    # Exit gently if no data found
    if 'status' in scenesgjson:
        logger.info(scenesgjson['message'])
        sys.exit(0)

    # Check with database
    if args.database:
        db = DB().initDB(args.database)
        conn = db.connect()
        cur = conn.cursor()

    # Check with gsconfig
    if args.catalog and args.store:
        catalog = Catalog(service_url=args.catalog[0], username=args.catalog[1], password=args.catalog[2])
        store = catalog.get_store(args.store[0].split(':')[1], args.store[0].split(':')[0])
        coverages = catalog.mosaic_coverages(store)

    # Check if granules already exist
    scenes = defaultdict(list)
    Landsat().download_file(args.output)
    if len(scenesgjson['features']) > 0:
        for s in scenesgjson['features']:
            for b in args.bands:
                if args.database:
                    r = db.lsat8_query(cur, b, str(s['properties']['sceneID']))
                    if not r:
                        scenes[s['properties']['sceneID']].append((b, False, s))
                    else:
                        if Landsat().download(str(s['properties']['sceneID']), str(b), args.output):
                            scenes[s['properties']['sceneID']].append((b, True, s))
                elif args.catalog and args.store:
                    for c in coverages['coverages']['coverage']:
                        granules = catalog.mosaic_granules(c['name'], store, filter='location like \'%' +
                                                                                    s['properties']['sceneID'] + '%\'')
                        if len(granules['features']) > 0:
                            logger.info('Scene ' + s['properties']['sceneID'] + ' for band ' + str(b) +
                                        ' already exists on database')
                            scenes[s['properties']['sceneID']].append((b, True, s))
                        else:
                            if Landsat().download(str(s['properties']['sceneID']), str(b), args.output):
                                scenes[s['properties']['sceneID']].append((b, False, s))
                else:
                    if Landsat().download(str(s['properties']['sceneID']), str(b), args.output):
                        scenes[s['properties']['sceneID']].append((b, False, s))
    if args.database:
        cur.close()
        conn.close()

    for s in scenes:
        for b in scenes[s]:
            if not b[1]:
                file.write(json.dumps(b) + '\n')
                file.flush()
    file.close()

if __name__ == '__main__':
    main()
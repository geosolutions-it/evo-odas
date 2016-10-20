#!/usr/bin/python

import os
import argparse
import json
import sys
from collections import defaultdict
from scripts.lsat8 import Landsat
from utils.db import DB

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
                                              'Ex: 432',
                                 default='432')
    parser.add_argument('output', help='Destination path to save images')

    parser.add_argument('--database', nargs=5, metavar=('db_name', 'db_host', 'db_port', 'db_user', 'db_password'),
                        help='Database connection params in the following order\n'
                             'db_name db_host db_port db_user db_password')

    args = parser.parse_args()

    # Search Scenes
    scenesgjson = Landsat().search(args)

    # Skip database
    if args.database:
        db = DB().initDB(args.database)
        conn = db.connect()
        cur = conn.cursor()

    # Check if granules already exist
    scenes = defaultdict(list)
    if len(scenesgjson['features']) > 0:
        for s in scenesgjson['features']:
            for b in list(args.bands):
                if args.database:
                    r = db.lsat8_query(cur, b, str(s['properties']['sceneID']))
                    if not r:
                        scenes[s['properties']['sceneID']].append((b, False, s))
                    else:
                        scenes[s['properties']['sceneID']].append((b, True, s))
                else:
                    scenes[s['properties']['sceneID']].append((b, False, s))
    if args.database:
        cur.close()
        conn.close()

    # Append ingestion file
    file = open(os.path.join(args.output, 'ingest.txt'), 'w')
    for s in scenes:
        for b in scenes[s]:
            if not b[1]:
                file.write(json.dumps(b) + '\n')
                file.flush()
    file.close()

    # Download Scenes
    Landsat().download(scenes, args.output)


if __name__ == '__main__':
    main()
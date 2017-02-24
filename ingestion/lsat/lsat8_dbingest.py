#!/usr/bin/python

import argparse
import json
import os

from ingestion.utils.db import DB


def main():

    parser = argparse.ArgumentParser(description="OWS12 Landsat script. Wrapper to landsat-util scripts.")

    parser.add_argument('-b', '--bands', help='Select the bands to inject into GeoServer mosaic\n'
                                              'Ex: 432',
                        default='432')
    parser.add_argument('files', help='Mosaic files path')

    parser.add_argument('--database', nargs=5, metavar=('db_name', 'db_host', 'db_port', 'db_user', 'db_password'),
                        help='Database connection params in the following order\n'
                             'db_name db_host db_port db_user db_password')

    args = parser.parse_args()

    db = DB().initDB(args.database)
    conn = db.connect()
    cur = conn.cursor()

    with open(os.path.join(args.files, 'ingest.txt'), 'r') as file:
        for l in file:
            row = json.loads(l)
            for b in list(args.bands):
                if b in row[0][0]:
                    db.lsat8_ingest(cur, b, row, args.files)

    cur.close()
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
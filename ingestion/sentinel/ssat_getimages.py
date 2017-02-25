#!/usr/bin/python

import argparse

from ingestion.scripts.sentinel import SentinelSat


def main():
    parser = argparse.ArgumentParser(description="OWS12 Sentinel products download script.")
    parser.add_argument('--auth', nargs=2, help='DataHub API authentication. eg: myuser mypass')
    parser.add_argument('-s', '--start',
                        help='Start Date - Most formats are accepted\n'
                             'e.g. Jun 12 2014 OR 06/12/2014')
    parser.add_argument('-e', '--end',
                        help='End Date - Most formats are accepted\n'
                             'e.g. Jun 12 2014 OR 06/12/2014')
    parser.add_argument('--geojson', help='Path to Geojson file containing area to search.', default='aoi.geojson')
    parser.add_argument('--query', help='Specific SciHub keywords to attach to query\n'
                                              'Ex: cloudcoverpercentage:[0 TO 20] platformname:Sentinel-2', nargs='+')
    parser.add_argument('--output', help='Destination path to save products packages')

    args = parser.parse_args()

    sentinel = SentinelSat()
    sentinel.auth_api(args.auth)

    sentinel.query_api(args.geojson, args.start, args.end, args.query)

    sentinel.download_products(args.output)

if __name__ == '__main__':
    main()
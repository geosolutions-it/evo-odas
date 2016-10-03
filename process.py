#!/usr/bin/python

import argparse
import os
import json
from scripts import gdal


def main():
    parser = argparse.ArgumentParser(description="OWS12 Landsat script. Wrapper to landsat-util scripts.")
    parser.add_argument('-b', '--bands', help='Select the bands to inject into GeoServer mosaic\n'
                                              'Ex: 432',
                        default='432')
    parser.add_argument('-r', '--resample', nargs=1, default='nearest',
                        choices=('nearest', 'average', 'gauss', 'cubic', 'cubicspline', 'lanczos', 'average_mp',
                                 'average_magphase', 'mode'),
                        help='Resample method to use on GDAL utils. default is nearest')
    parser.add_argument('-c', '--config', nargs='?', help='Specific GDAL configuration string\n'
                                                          'Ex: --config COMPRESS_OVERVIEW JPEG')
    parser.add_argument('-o', '--overviews', nargs=1,
                        help='Overviews to add to the target image')
    parser.add_argument('-w', '--warp', nargs=1,
                        help='The projection EPSG code to use for gdalwarp')
    parser.add_argument('files', help='Mosaic files path')

    args = parser.parse_args()

    gd = gdal.GDAL()
    # Add common options
    gd.rmethod = args.resample[0]

    if args.warp:
        with open(os.path.join(args.files, 'ingest.txt'), 'r') as file:
            for l in file:
                row = json.loads(l)
                for b in list(args.bands):
                    if b in row[0][0]:
                        granule = os.path.join(args.files, str(row[2]['properties']['sceneID']),
                                     str(row[2]['properties']['sceneID']) + '_B' + b + '.TIF')
                        output_granule = granule.replace('.TIF', '_WARPED.TIF')
                        # TODO: Enable parallel processing
                        gd.warp(granule, output_granule, args.warp[0])
                        # Stick with original filenames
                        os.remove(granule)
                        os.rename(output_granule, granule)

    if args.overviews:
        scales = args.overviews[0].split(',')
        with open(os.path.join(args.files, 'ingest.txt'), 'r') as file:
            for l in file:
                row = json.loads(l)
                for b in list(args.bands):
                    if b in row[0][0]:
                        granule = os.path.join(args.files, str(row[2]['properties']['sceneID']),
                                     str(row[2]['properties']['sceneID']) + '_B' + b + '.TIF')
                        # TODO: Enable parallel processing
                        gd.addOverviews(granule, scales, args.config)


if __name__ == '__main__':
    main()
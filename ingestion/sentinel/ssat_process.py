#!/usr/bin/python

import argparse
import os
import sys

from ingestion.utils.utils import initLogger

from ingestion.scripts.gdal import GDAL
from ingestion.scripts.sentinel import SentinelSat

# Make logger global here
logger = initLogger()

def main():
    parser = argparse.ArgumentParser(description="OWS12 Sentinel preprocess granules script.")
    parser.add_argument('-b', '--bands', help='List of bands to process.\n'
                                              'e.g. 4 3 2', default='4 3 2', type=int, nargs='+')
    parser.add_argument('-r', '--resample', nargs=1, default='nearest',
                        choices=('nearest', 'average', 'gauss', 'cubic', 'cubicspline', 'lanczos', 'average_mp',
                                 'average_magphase', 'mode'),
                        help='Resample method to use on GDAL utils. default is nearest')
    parser.add_argument('-c', '--config', nargs='?', help='Specific GDAL configuration string\n'
                                                          'e.g. --config COMPRESS_OVERVIEW DEFLATE')
    parser.add_argument('-o', '--overviews', help='Overviews to add to the target image.\n'
                                                  'e.g. 2 4 8 16 32 64',
                        type=int, nargs='+')
    parser.add_argument('-w', '--warp', nargs=1,
                        help='The projection EPSG code to use for gdalwarp')
    parser.add_argument('--download', help='Path to previously saved products packages')
    parser.add_argument('--output', help='Destination path for the processed images')

    args = parser.parse_args()

    gd = GDAL()
    # Add common options
    gd.rmethod = args.resample[0]

    sentinel = SentinelSat()
    sentinel.download_path = args.download
    sentinel.mosaic_path = args.output

    # Before we proceed, check if we retrieved any files during previous download step
    if not os.path.exists(os.path.join(args.download, sentinel.products_list)):
        logger.info('Missing file from previous download job.\nPlease run this job first!')
        sys.exit(1)
    num_lines = sum(1 for l in open(os.path.join(args.download, sentinel.products_list)))
    if num_lines == 0:
        logger.info('Skipping processing step. No files to process found!')
        sys.exit(0)

    try:
        sentinel.unpack_products(sentinel.download_path)
        if args.warp:
            warp_options = '-srcnodata 0 -dstnodata 0 -co BLOCKXSIZE=512 -co BLOCKYSIZE=512 -co TILED=YES ' \
                           '-wo OPTIMIZE_SIZE=YES -co COMPRESS=DEFLATE -of GTiff'
            sentinel.warp_granules(sentinel.download_path, sentinel.mosaic_path, args.bands, gd, args.warp[0], warp_options)
        if args.overviews:
            sentinel.overviews_granules(sentinel.mosaic_path, args.bands, gd, args.overviews, args.config)
        sentinel.remove_products(sentinel.download_path)

    except Exception, e:
        logger.error(e)

if __name__ == '__main__':
    main()
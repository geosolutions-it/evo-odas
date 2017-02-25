#!/usr/bin/python

import os

from landsat.downloader import Downloader
from landsat.search import Search
from ingestion.utils.utils import initLogger

# Make logger global here
logger = initLogger()


class Landsat(object):
    def __init__(self):
        self.cloud_min = 0
        self.geojson = True
        self.download_list = 'download_files.txt'

    def download_file(self, download_dir):
        open(os.path.join(download_dir, self.download_list), 'w')

    def search(self, args):
        try:
            logger.debug('Executing landsat search with params %s' % args)

            l_search = Search()

            response = l_search.search(paths_rows=args.pathrow, lat=args.lat, lon=args.lon, start_date=args.start,
                                       end_date=args.end, cloud_min=self.cloud_min, cloud_max=args.cloud,
                                       limit=args.limit, geojson=self.geojson)
            return response
        except Exception as e:
            logger.error('Exception querying landsat images with stacktrace %s' % (str(e)))
            return False

    def download(self, scene, band, download_dir):
        try:
            f = open(os.path.join(download_dir, self.download_list), 'a')
            l_download = Downloader(download_dir=download_dir)
            s = []
            b = []
            s.append(scene)
            b.append(band)
            l_download.download(s, b)
            f.write(os.path.join(download_dir, scene, scene + '_B' + band + '.TIF\n'))
            f.flush()
            f.close()
            return True
        except Exception as e:
            logger.error('Exception downloading landsat images with stacktrace %s' % (str(e)))
            return False
#!/usr/bin/python

import os
import sys
from landsat.downloader import Downloader
from landsat.search import Search
from utils.utils import initLogger

# Make logger global here
logger = initLogger()


class Landsat(object):
    def __init__(self):
        self.cloud_min = 0
        self.geojson = True
        self.download_list = 'download_files.txt'

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

    def download(self, response_dict, download_dir):
        try:
            f = open(os.path.join(download_dir, self.download_list), 'w')
            l_download = Downloader(download_dir=download_dir)
            for s in response_dict:
                scene = []
                scene.append(str(s))
                for b in response_dict[s]:
                    if not b[1]:
                        # Download per band, in case only 1 band missing...
                        band = []
                        band.append(b[0])
                        l_download.download(scene, band)
                        f.write(os.path.join(download_dir, str(s), str(s) + '_B' + str(b[0]) + '.TIF\n'))
                        f.flush()
            f.close()
        except Exception as e:
            logger.error('Exception downloading landsat images with stacktrace %s' % (str(e)))
            return False
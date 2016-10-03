#!/usr/bin/python

from landsat.search import Search
from utils.utils import initLogger

# Make logger global here
logger = initLogger()


class LandsatSearch(object):
    def __init__(self):
        self.cloud_min = 0
        self.geojson = True

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
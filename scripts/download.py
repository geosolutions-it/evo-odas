#!/usr/bin/python

from landsat.downloader import Downloader
from utils.utils import initLogger

# Make logger global here
logger = initLogger()


class LandsatDownload(object):

    def download(self, response_dict, download_dir):
        try:
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
        except Exception as e:
            logger.error('Exception downloading landsat images with stacktrace %s' % (str(e)))
            return False
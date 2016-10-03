#!/usr/bin/python

import os
from landsat.downloader import Downloader
from utils.utils import initLogger

# Make logger global here
logger = initLogger()


class LandsatDownload(object):

    def download(self, response_dict, download_dir):
        try:
            f = open(os.path.join(download_dir, 'download_files.txt'), 'w')
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
                        f.write(os.path.join(download_dir, str(s), str(s) + '_B' + str(b[0]) + '.TIF'))
                        f.flush()
            f.close()
        except Exception as e:
            logger.error('Exception downloading landsat images with stacktrace %s' % (str(e)))
            return False
#!/usr/bin/python

import os
import sys
import zipfile
import shutil
import fnmatch
from utils.utils import initLogger
from sentinelsat.sentinel import SentinelAPI, get_coordinates, SentinelAPIError
from datetime import date, timedelta, datetime, time
from scripts.gdal import GDAL

# Make logger global here
logger = initLogger()


class SentinelSat(object):
    def __init__(self):
        self.api = ''
        self.start_date = date.today() - timedelta(1)
        self.end_date = date.today()
        self.api_url = 'https://scihub.copernicus.eu/dhus/'
        self.products_list = 'sentinel_products.txt'
        self.processed_granules = 'processed_granules.txt'

    def auth_api(self, auth):
        self.api = SentinelAPI(auth[0], auth[1], self.api_url)
        return self.api

    def query_api(self, area, start_date, end_date, qargs):
        if not start_date and not end_date:
            start_date = self.start_date
            end_date = self.end_date
        else:
            start_date = datetime.strptime(start_date, "%d/%m/%Y").date()
            end_date = datetime.strptime(end_date, "%d/%m/%Y").date()

        keywords = {}
        for k in qargs:
            k = k.split(':')
            keywords[k[0]] = k[1]
        try:
            self.api.query(get_coordinates(area), start_date, end_date, **keywords)
        except SentinelAPIError, e:
            logger.info(e.msg)

    def open_file(self, download_path, filename, type):
        return open(os.path.join(download_path, filename), type)

    def download_products(self, download_path):
        prod_file = self.open_file(download_path, self.products_list, 'w')
        if len(self.api.products) > 0:
            for p in self.api.products:
                if not os.path.exists(os.path.join(download_path, p['title'] + '.zip')):
                    self.api.download(p['id'], download_path)
                    prod_file.write(p['title'] + '\n')
                    prod_file.flush()
                else:
                    logger.info('Product %s already downloaded.', p['title'])
            prod_file.close()
        else:
            logger.info('No images available for the specified query.')
            sys.exit(0)

    def unpack_products(self, download_path):
        prod_file = self.open_file(download_path, self.products_list, 'r')
        for l in prod_file:
            with zipfile.ZipFile(os.path.join(download_path, l.rstrip('\n') + '.zip')) as z:
                z.extractall(path=download_path)
        prod_file.close()

    def remove_products(self, download_path):
        prod_file = self.open_file(download_path, self.products_list, 'r')
        for l in prod_file:
            shutil.rmtree(os.path.join(download_path, l.rstrip('\n') + '.SAFE'))

    def get_s2_package_granules(self, download_path, package):
        granules = []
        # Navigate to granules folder (Sentinel 2 package)
        for d in os.listdir(os.path.join(download_path, package + '.SAFE', 'GRANULE')):
            for f in os.listdir(os.path.join(download_path, package + '.SAFE', 'GRANULE', d, 'IMG_DATA')):
                granules.append(os.path.join(download_path, package + '.SAFE', 'GRANULE', d, 'IMG_DATA', f))
        return granules

    def copy_granules_s2(self, download_path, granules_path, bands):
        prod_file = self.open_file(download_path, self.products_list, 'r')
        granules_file = self.open_file(granules_path, self.processed_granules, 'w')
        for l in prod_file:
            for f in self.get_s2_package_granules(download_path, l.rstrip('\n')):
                for b in bands:
                    b = 'B0' + str(b) if b < 10 else 'B' + str(b)
                    if fnmatch.fnmatch(f, '*' + b + '.tif'):
                        shutil.move(f, granules_path)
                        granules_file.write(os.path.join(granules_path, f.split('/')[-1:][0]) + '\n')
                        granules_file.flush()
                        if b == 'B08':
                            shutil.move(f.replace('B08', 'B8A'), granules_path)
                            granules_file.write(os.path.join(granules_path, f.replace('B08', 'B8A')
                                                              .split('/')[-1:][0]) + '\n')
                            granules_file.flush()
        granules_file.close()
        prod_file.close()

    def warp_granules(self, download_path, bands, gdobj, t_epsg, options):
        prod_file = self.open_file(download_path, self.products_list, 'r')
        for l in prod_file:
            for f in self.get_s2_package_granules(download_path, l.rstrip('\n')):
                for b in bands:
                    b = 'B0' + str(b) if b < 10 else 'B' + str(b)
                    if fnmatch.fnmatch(f, '*' + b + '.jp2'):
                        output_granule = f.replace('.jp2', '.tif')
                        gdobj.warp(inputf=f, outputf=os.path.join(download_path, output_granule), t_srs=t_epsg,
                                   options=options)
                        if b == 'B08':
                            output_granule = output_granule.replace('B08', 'B8A')
                            gdobj.warp(inputf=f.replace('B08', 'B8A'),
                                       outputf=os.path.join(download_path, output_granule), t_srs=t_epsg,
                                       options=options)
                    else:
                        continue
        prod_file.close()

    def overviews_granules(self, download_path, bands, gdobj, scales, options):
        prod_file = self.open_file(download_path, self.products_list, 'r')
        for l in prod_file:
            for f in self.get_s2_package_granules(download_path, l.rstrip('\n')):
                for b in bands:
                    b = 'B0' + str(b) if b < 10 else 'B' + str(b)
                    if fnmatch.fnmatch(f, '*' + b + '.tif'):
                        gdobj.addOverviews(file=f, scales=scales, configs=options)
                        if b == 'B08':
                            gdobj.addOverviews(file=f.replace('B08', 'B8A'), scales=scales, configs=options)
                    else:
                        continue
        prod_file.close()

    def insert_granules(self, granules_path, catalog, coverages, store):
        granules_file = self.open_file(granules_path, self.processed_granules, 'r')
        with open(granules_file) as g:
            for l in g:
                ingest = True
                gf = l.replace('\n', '').split('/')[-1]
                for c in coverages['coverages']['coverage']:
                    granules = catalog.mosaic_granules(c['name'], store, filter='location like \'%' + gf + '%\'')
                    if len(granules['features']) > 0:
                        logger.info('File ' + gf + ' already exists on database')
                        ingest = False
                if ingest:
                    logger.info('Ingesting granule ' + gf)
                    g = 'file://' + l.replace('\n', '')
                    catalog.harvest_externalgranule(g, store)
        granules_file.close()

    def delete_granules(self, catalog, coverages, store, range):
        today = time.strftime('%Y-%m-%dT00:00:00')
        for c in coverages['coverages']['coverage']:
            granules = catalog.mosaic_granules(c['name'], store,
                                               filter='time before P' + str(range) + 'M/' + today)
            if len(granules['features']) > 0:
                for g in granules['features']:
                    logger.info('Deleting granule ' + g['properties']['location'])
                    catalog.mosaic_delete_granule(c['name'], store, g['id'])
            else:
                logger.info('No granules to delete!')

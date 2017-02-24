#!/usr/bin/python

import fnmatch
import os
import shutil
import sys
import time
import zipfile
from datetime import date, timedelta, datetime
from sentinelsat.sentinel import SentinelAPI, get_coordinates, SentinelAPIError
from ingestion.utils.utils import initLogger

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
        self.download_path = ''
        self.mosaic_path = ''

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

    def get_mask(self, granule, band, output):
        from osgeo import ogr

        folder = granule.split('IMG_DATA')[0]
        source_gml = os.path.join(folder, 'QI_DATA', 'MSK_DETFOO_' + str(band) + '.gml')

        source = ogr.Open(source_gml)

        layerS = source.GetLayer()

        s_srs = layerS.GetNextFeature().GetGeometryRef().GetSpatialReference().GetAuthorityCode("PROJCS")

        os.system('ogr2ogr -F GML -s_srs EPSG:%s -t_srs EPSG:4326 %s %s'
                  % (s_srs, source_gml.replace('.gml', '_wgs84.gml'), source_gml))

        target = ogr.Open(source_gml.replace('.gml', '_wgs84.gml'))

        layerT = target.GetLayer()

        features = layerT.GetFeatureCount()
        multi = ogr.Geometry(ogr.wkbMultiPolygon)

        for f in range(0, features):
            multi.AddGeometry(layerT.GetFeature(f).GetGeometryRef())

        sidecar = granule.split('IMG_DATA/')[1].replace('.tif', '.wkt')

        with open(os.path.join(output, sidecar), 'w') as wkt_sidecar:
            wkt_sidecar.write(multi.UnionCascaded().ExportToWkt())
            wkt_sidecar.flush()
            wkt_sidecar.close()

    def warp_granules(self, download_path, granules_path, bands, gdobj, t_epsg, options, rgb=True, mask=True):
        prod_file = self.open_file(download_path, self.products_list, 'r')
        granules_file = self.open_file(granules_path, self.processed_granules, 'w')
        for l in prod_file:
            for f in self.get_s2_package_granules(download_path, l.rstrip('\n')):
                for b in bands:
                    b = 'B0' + str(b) if b < 10 else 'B' + str(b)
                    if fnmatch.fnmatch(f, '*' + b + '.jp2'):
                        output_granule = os.path.join(granules_path, f.replace('.jp2', '.tif').split('/')[-1:][0])
                        gdobj.warp(inputf=f, outputf=output_granule, t_srs=t_epsg,
                                   options=options)
                        granules_file.write(output_granule + '\n')
                        granules_file.flush()
                        if mask:
                            self.get_mask(f.replace('.jp2', '.tif'), b, granules_path)
                        if b == 'B08':
                            output_granule = output_granule.replace('B08', 'B8A')
                            gdobj.warp(inputf=f.replace('B08', 'B8A'), outputf=output_granule, t_srs=t_epsg,
                                       options=options)
                            granules_file.write(output_granule + '\n')
                            granules_file.flush()
                            if mask:
                                self.get_mask(f.replace('B08', 'B8A').replace('.jp2', '.tif'), b.replace('B08', 'B8A'),
                                              granules_path)
                        continue
                # Process RGB files
                if rgb:
                    if fnmatch.fnmatch(f, '*TCI.jp2'):
                        output_granule = os.path.join(granules_path, f.replace('.jp2', '.tif').split('/')[-1:][0])
                        gdobj.warp(inputf=f, outputf=output_granule, t_srs=t_epsg,
                                   options=options)
                        granules_file.write(output_granule + '\n')
                        granules_file.flush()
                        if mask:
                            self.get_mask(f.replace('.jp2', '.tif'), b, granules_path)
                        continue
        prod_file.close()
        granules_file.close()

    def overviews_granules(self, granules_path, bands, gdobj, scales, options, rgb=True):
        granules_file = self.open_file(granules_path, self.processed_granules, 'r')
        for l in granules_file:
            if fnmatch.fnmatch(l.rstrip('\n'), '*TCI.tif'):
                gdobj.addOverviews(file=l.rstrip('\n'), scales=scales, configs=options)
            else:
                gdobj.addOverviews(file=l.rstrip('\n'), scales=scales, configs=options)
        granules_file.close()

    def insert_granules(self, granules_path, catalog, coverages, store):
        granules_file = self.open_file(granules_path, self.processed_granules, 'r')
        with granules_file as g:
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
                                               filter='time before P' + str(range[0]) + 'M/' + today)
            if len(granules['features']) > 0:
                for g in granules['features']:
                    logger.info('Deleting granule ' + g['properties']['location'])
                    catalog.mosaic_delete_granule(c['name'], store, g['id'])
            else:
                logger.info('No granules to delete!')

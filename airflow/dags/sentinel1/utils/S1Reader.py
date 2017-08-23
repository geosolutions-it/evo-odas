#!/usr/bin/env python
import sys
import os
from pprint import pprint
import logging
import xml.etree.ElementTree as ET
import geojson
import shapely.wkt
import utils
try:
    from osgeo import gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules, install gdal with python bindings')

log = logging.getLogger(__name__)

class S1GDALReader:

    def __init__(self, sentinel1_product_zip_path):
        self.product_zip_path = sentinel1_product_zip_path
        sentinel1_product_dir = os.path.dirname(sentinel1_product_zip_path)
        sentinel1_product_zipname = os.path.basename(sentinel1_product_zip_path)
        self.product_dir = sentinel1_product_dir
        self.sentinel1_product_zipname = sentinel1_product_zipname
        self.granule_identifier, _ = os.path.splitext(sentinel1_product_zipname)

        manifest_path = utils.extract_manifest_from_zip(sentinel1_product_zip_path)
        self.manifest_tree = ET.parse(manifest_path)
        try:
            os.remove(manifest_path)
            os.rmdir(os.path.dirname(manifest_path))
        except:
            log.warn("Cannot cleanup manifest directory")

        #sentinel1_safe_pkg_path = "/vsizip/{}/{}.SAFE/manifest.safe".format(self.product_zip_path, self.granule_identifier)

        #self.safe_package_path = sentinel1_safe_pkg_path
        #self.datastore = gdal.Open(sentinel1_safe_pkg_path)

    def get_metadata(self):
        manifest_zip_path = utils.get_manifest_zip_path(self.product_zip_path)
        datastore = gdal.Open(manifest_zip_path)
        metadata_dict = datastore.GetMetadata()
        metadata_dict['NAME'] = self.granule_identifier
        return metadata_dict

    def get_footprint(self):
        GML_NS = "{http://www.opengis.net/gml}"
        gml_coordinates = ""
        for el in self.manifest_tree.iter(GML_NS + "coordinates"):
            gml_coordinates = el.text
        wkt_coordinates = gml_coordinates.replace(",", ";")
        wkt_coordinates = wkt_coordinates.replace(" ", ",")
        wkt_coordinates = wkt_coordinates.replace(";", " ")
        wkt_coordinates = wkt_coordinates + "," + wkt_coordinates.split(",")[0]
        s = "POLYGON ((" + wkt_coordinates + "))"
        log.info("stringa s = " + s)
        g1 = shapely.wkt.loads(s)
        g2 = geojson.Feature(geometry=g1, properties={})
        print type(g2.geometry)
        return g2.geometry

    def get_quicklook(self):
        return  utils.extract_file_from_zip("quick-look.png", self.product_zip_path)

    def get_preview_image(self):
        return os.path.join(self.safe_package_path, "preview", "quick-look.png")

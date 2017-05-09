#!/usr/bin/env python
from os import path
from lxml.etree import parse, fromstring
try:
    from osgeo import gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules, install gdal 2.1.3 with python bindings')

class S1GDALReader:

    def __init__(self, sentinel1_safe_pkg_path):
        splitted = sentinel1_safe_pkg_path.split('/')
        self.safe_package_path = sentinel1_safe_pkg_path
        self.granule_identifier = splitted[len(splitted)-1].split('.')[0]
        self.datastore = gdal.Open(sentinel1_safe_pkg_path)

    def get_metadata(self):
        metadata_dict = self.datastore.GetMetadata()
        metadata_dict['NAME'] = self.granule_identifier
        return metadata_dict
    #def get_footprint():
    #    self.datastore.

    def get_footprint(self):
        GML_NS = "{http://www.opengis.net/gml}"
        gml_coordinates = ""
        with open(path.join(self.safe_package_path, "manifest.safe")) as f:
            obj = parse(f)
            for el in obj.iter(GML_NS + "coordinates"):
                gml_coordinates = el.text
        wkt_coordinates = gml_coordinates.replace(",", ";")
        wkt_coordinates = wkt_coordinates.replace(" ", ",")
        wkt_coordinates = wkt_coordinates.replace(";", " ")
        wkt_coordinates = wkt_coordinates + "," + wkt_coordinates.split(",")[0]
        return "POLYGON ((" + wkt_coordinates + "))"

    def get_preview_image(self):
        return path.join(self.safe_package_path, "preview", "quick-look.png")

#!/usr/bin/env python
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

    def get_metadata():
        metadata_dict = self.datastore.GetMetadata()
        metadata_dict['NAME'] = self.granule_identifier

    #def get_footprint():
    #    self.datastore.

    def get_preview_image():
        return os.join(self.safe_package_path, "preview", "quick-look.png")

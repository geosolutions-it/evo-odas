from datetime import datetime, timedelta
import os
import config

#
# DAG
#

dag_schedule_interval='@hourly'

#
# Collection
#
id = "S1_GRD_1SDV"
filename_filter = "S1?_*_GRD?_1SDV*"
platformname = 'Sentinel-1'
collection_dir = os.path.join(config.base_dir, platformname, id)
download_dir = os.path.join(collection_dir,"download")
process_dir = os.path.join(collection_dir,"process")
repository_dir = os.path.join(collection_dir,"repository")
original_package_upload_dir = os.path.join(collection_dir,"upload")
original_package_download_base_url = "http://cloudsdi.geo-solutions.it/data/sentinel/sentinel1/grd/S1_GRD_1SDV/"

#
# DHUS specific
#
dhus_filter_max = 5
dhus_download_max = 1
dhus_search_bbox = os.path.join(config.regions_base_dir,'europe.geojson')
dhus_search_filename = filename_filter
dhus_search_startdate = datetime.today() - timedelta(days=14)
dhus_search_startdate = dhus_search_startdate.isoformat() + 'Z'
dhus_search_enddate = datetime.now().isoformat() + 'Z'
dhus_search_orderby = '+ingestiondate'
dhus_search_keywords = {
        'filename': filename_filter,
        'platformname': platformname
}

#
# GeoServer
#
geoserver_workspace = "sentinel"
geoserver_featuretype = "SENTINEL1_FT"

geoserver_oseo_wfs_format = "application/json"
geoserver_oseo_wfs_version = "2.0.0"
geoserver_layer = "SENTINEL1"
geoserver_coverage = "SENTINEL1"
geoserver_oseo_collection="SENTINEL1"
geoserver_oseo_wms_width = 768
geoserver_oseo_wms_height = 768
geoserver_oseo_wms_format = "image/png"
geoserver_oseo_wms_version = "1.3.0"
geoserver_oseo_wcs_scale_i = 0.01
geoserver_oseo_wcs_scale_j = 0.01
geoserver_oseo_wcs_format = "image/tiff"
geoserver_oseo_wcs_version = "2.0.1"

#
# Product
#

bands_dict = {"hv":"hv","hh":"hh", "vv":"vv", "vh":"vh"}

try:
    from override.s1_grd_1sdv import *
except:
    pass

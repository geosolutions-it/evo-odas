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
id = "S2_MSI_L1C"
filename_filter = "S2*_MSIL1C*"
platformname = 'Sentinel-2'
collection_dir = os.path.join(config.base_dir, platformname, id)
download_dir = os.path.join(collection_dir,"download")
process_dir = os.path.join(collection_dir,"process")
repository_dir = os.path.join(collection_dir,"repository")
original_package_upload_dir = os.path.join(collection_dir,"upload")
original_package_download_base_url = "http://geoserver.cloudsdi.geo-solutions.it/data/sentinel/sentinel2/S2_MSI_L1C"

#
# DHUS specific
#
dhus_filter_max = 5
dhus_download_max = dhus_filter_max
dhus_search_bbox = os.path.join(config.regions_base_dir,'europe.geojson')
dhus_search_filename = filename_filter
dhus_search_startdate = datetime.today() - timedelta(days=4)
dhus_search_startdate = dhus_search_startdate.isoformat() + 'Z'
dhus_search_enddate = datetime.now().isoformat() + 'Z'
dhus_search_orderby = '-ingestiondate,+cloudcoverpercentage'
dhus_search_keywords = {
        'filename': filename_filter,
        'platformname': platformname,
        'orbitdirection':'Descending',
        'cloudcoverpercentage':'[0 TO 10]'    
}

#
# GeoServer
#
geoserver_workspace = "sentinel"
geoserver_featuretype = "product"
geoserver_oseo_wfs_format = "application/json"
geoserver_oseo_wfs_version = "2.0.0"
geoserver_layer = "SENTINEL2"
geoserver_coverage = "SENTINEL2"
geoserver_oseo_collection="SENTINEL2"
geoserver_oseo_wms_width = 512
geoserver_oseo_wms_height = 512
geoserver_oseo_wms_format = "image/jpeg"
geoserver_oseo_wms_version = "1.3.0"
geoserver_oseo_wcs_scale_i = 0.1
geoserver_oseo_wcs_scale_j = 0.1
geoserver_oseo_wcs_format = "image/tiff"
geoserver_oseo_wcs_version = "2.0.1"

#
# Product
#
bands_res = {
    '10':("B02","B03","B04","B08","TCI"),
    '20':("B05","B06","B07","B8A","B11","B12"),
    '60':("B01","B09","B10")
}
bands_dict = {'B01':'B01','B02':'B02','B03':'B03','B04':'B04','B05':'B05','B06':'B06','B07':'B07',
              'B08':'B08','B8A':'B8A','B09':'B09','B10':'B10','B11':'B11','B12':'B12','TCI':'TCI'}

try:
    from override.s2_msi_l1c import *
except:
    pass

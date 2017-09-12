from datetime import datetime, timedelta
import os
import config

#
# Collection
#
id = "S2_MSI_L1C"
filename_filter = "S2*_MSIL1C*"
platformname = 'Sentinel-2'

collection_dir = os.path.join(config.base_dir, platformname, id)
download_dir = os.path.join(collection_dir,"download")
process_dir = os.path.join(collection_dir,"process")
upload_dir = os.path.join(collection_dir,"upload")
repository_dir = os.path.join(config.repository_base_dir, "SENTINEL/S2/", id, "v0")

#
# DHUS specific
#
dhus_filter_max = 2
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
geoserver_workspace = "evoodas"
geoserver_layer = "SENTINEL2"
geoserver_coverage = "SENTINEL2"
geoserver_oseo_collection="SENTINEL2"
geoserver_oseo_wms_width = 512
geoserver_oseo_wms_height = 512
geoserver_oseo_wms_format = "JPEG2000"

#
# Product
#
bands_res = {
    '10':("B02","B03","B04","B08"),
    '20':("B05","B06","B07","B8A","B11","B12"),
    '60':("B01","B09","B10")
}
bands_dict = {'B01':1,'B02':2,'B03':3,'B04':4,'B05':5,'B06':6,'B07':7,'B08':8,'B8A':9,'B09':10,'B10':11,'B11':12,'B12':13,'TCI':14}

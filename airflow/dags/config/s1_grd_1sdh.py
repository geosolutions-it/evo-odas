from datetime import datetime, timedelta
import os
import config

#
# Collection
#
id = "S1_GRD_1SDH"
filename_filter = "S1?_EW_GRDM_1SDH*"
platformname = 'Sentinel-1'

collection_dir = os.path.join(config.base_dir, platformname, id)
download_dir = os.path.join(collection_dir,"download")
process_dir = os.path.join(collection_dir,"process")
upload_dir = os.path.join(collection_dir,"upload")
repository_dir = os.path.join(config.repository_base_dir, "SENTINEL/S1/", id, "v0")

#
# DHUS specific
#
dhus_filter_max = 2
dhus_search_bbox = os.path.join(config.regions_base_dir,'europe.geojson')
dhus_search_filename = filename_filter
dhus_search_startdate = datetime.today() - timedelta(days=4)
dhus_search_startdate = dhus_search_startdate.isoformat() + 'Z'
dhus_search_enddate = datetime.now().isoformat() + 'Z'
dhus_search_orderby = '-ingestiondate'
dhus_search_keywords = {
        'filename': filename_filter,
        'platformname': platformname,
        'orbitdirection':'Descending',
}
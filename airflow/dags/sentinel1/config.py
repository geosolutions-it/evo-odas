import os
from datetime import datetime, timedelta

geoserver_url = "http://geoserver.cloudsdi.geo-solutions.it/geoserver"

platformname= 'Sentinel-1'
download_base_dir= '/var/data/download/'
download_dir = os.path.join(download_base_dir, platformname, 'GRD')

# yesterday at beginning of day
startdate= datetime.now() - timedelta(days=1)
startdate= startdate.replace(hour=0, minute=0, second=0, microsecond=0)
startdate = startdate.isoformat() + 'Z'

# yesterday at end of day
enddate= datetime.now() - timedelta(days=1)
enddate= enddate.replace(hour=23, minute=59, second=59, microsecond=999999)
enddate= enddate.isoformat() + 'Z'


ew_grdm_1sdh_config = {
    'download_max': '1',
    'geojson_bbox': '/var/data/regions/daraa.geojson',
    'startdate': startdate,
    'enddate': enddate,
    'platformname': platformname,
    'filename': 'S1?_EW_GRDM_1SDH*',
    'download_dir': download_dir,
    'granules_upload_dir': '/efs/geoserver_data/coverages/sentinel/sentinel1/grd/EW_GRDM_1SDH'
}

ew_grdm_1sdv_config = {
    'download_max': '1',
    'geojson_bbox': '/var/data/regions/germany.geojson',
    'startdate': startdate,
    'enddate': enddate,
    'platformname': platformname,
    'filename': 'S1?_*_GRD?_1SDV*',
    'download_dir': download_dir,
    'granules_upload_dir': '/efs/geoserver_data/coverages/sentinel/sentinel1/grd/EW_GRDM_1SDV'
}

sentinel2_config = {
    'download_max': '1',
    'geojson_bbox': '/var/data/regions/germany.geojson',
    'startdate': (datetime.today() - timedelta(days=10)).isoformat() + 'Z',
    'enddate': enddate,
    'platformname': 'Sentinel-2',
    'filename': 'S2A_MSIL1C*',
    'download_dir': os.path.join(download_base_dir, "Sentinel-2"),
    'granules_upload_dir': "/var/data/download/uploads"
}

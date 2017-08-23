import os
from datetime import datetime, timedelta

platformname= 'Sentinel-1'
download_base_dir= '/var/data/download/'
download_dir = os.path.join(download_base_dir, platformname, 'GRD')
startdate= datetime.today() - timedelta(days=30)
startdate = startdate.isoformat() + 'Z'
enddate= datetime.now().isoformat() + 'Z'


ew_grdm_1sdh_config = {
    'download_max': '1',
    'geojson_bbox': '/var/data/regions/europe.geojson',
    'startdate': (datetime.today() - timedelta(days=30)).isoformat() + 'Z',
    'enddate': enddate,
    'platformname': platformname,
    'filename': 'S1?_EW_GRDM_1SDH*',
    'download_dir': download_dir
}

ew_grdm_1sdv_config = {
    'download_max': '1',
    'geojson_bbox': '/var/data/regions/europe.geojson',
    'startdate': startdate,
    'enddate': enddate,
    'platformname': platformname,
    'filename': 'S1?_EW_GRDM_1SDV*',
    'download_dir': download_dir
}

sentinel2_config = {
    'download_max': '1',
    'geojson_bbox': '/var/data/regions/germany.geojson',
    'startdate': (datetime.today() - timedelta(days=10)).isoformat() + 'Z',
    'enddate': enddate,
    'platformname': 'Sentinel-2',
    'filename': 'S2A_MSIL1C*',
    'download_dir': os.path.join(download_base_dir, "Sentinel-2")
}
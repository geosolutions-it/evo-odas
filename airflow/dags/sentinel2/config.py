from datetime import datetime, timedelta
import os

# yesterday at beginning of day
startdate= datetime.now() - timedelta(days=1)
startdate= startdate.replace(hour=0, minute=0, second=0, microsecond=0)
startdate = startdate.isoformat() + 'Z'

# yesterday at end of day
enddate= datetime.now() - timedelta(days=1)
enddate= enddate.replace(hour=23, minute=59, second=59, microsecond=999999)
enddate= enddate.isoformat() + 'Z'

# download base dir
download_base_dir= '/var/data/download/'

geoserver_url = "http://geoserver.cloudsdi.geo-solutions.it/geoserver"
geoserver_collection_name="SENTINEL2"

sentinel2_config = {
    'download_max': '1',
    'geojson_bbox': '/var/data/regions/germany.geojson',
    'startdate': (datetime.today() - timedelta(days=10)).isoformat() + 'Z',
    'enddate': enddate,
    'search_keywords' : {
        'filename': 'S2A_MSIL1C*',
        'platformname': 'Sentinel-2',
        'orbitdirection':'Descending',
        'cloudcoverpercentage':'[0 TO 5]'
    },
    'download_dir': os.path.join(download_base_dir, "Sentinel-2"),
    'granules_upload_dir': "/efs/geoserver_data/coverages/sentinel/sentinel2/sentinel2",
    'bands_res': {'10':("B02","B03","B04","B08"),'20':("B05","B06","B07","B8A","B11","B12"),'60':("B01","B09","B10")},
    'bands_dict': {'B01':1,'B02':2,'B03':3,'B04':4,'B05':5,'B06':6,'B07':7,'B08':8,'B8A':9,'B09':10,'B10':11,'B11':12,'B12':13,'TCI':14},
    'product_zip_target_dir':"/var/data/download/Sentinel-2", 
    'rsync_hostname': 'geoserver.cloudsdi.geo-solutions.it',
    'rsync_username': 'ec2-user',
    'rsync_ssh_key' : '/usr/local/airflow/id_rsa',
    'GS_WORKSPACE'  : 'S2XYZ_Workspace',
    'GS_LAYER'      : 'S2XYZ_Layer',
    'GS_WMS_WIDTH'  : '512', 
    'GS_WMS_HEIGHT' : '512',
    'GS_WMS_FORMAT' : 'JPEG2000',
    'coverage_id'   : 'S2_XYZ_CID'
}

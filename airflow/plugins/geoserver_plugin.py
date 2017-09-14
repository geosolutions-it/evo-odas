import logging
import requests
from pprint import pprint, pformat
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import XCOM_RETURN_KEY
import config.xcom_keys as xk
from geoserver.catalog import Catalog

import sys
reload(sys)
sys.setdefaultencoding('utf8')

log = logging.getLogger(__name__)

class GSAddMosaicGranule(BaseOperator):

    @apply_defaults
    def __init__(self, geoserver_rest_url, gs_user, gs_password, imagemosaic_storename, mosaic_path, index, *args, **kwargs):
        self.catalog = Catalog(geoserver_rest_url, gs_user, gs_password)
        self.store_name = imagemosaic_storename
        self.mosaic_path = mosaic_path
        self.index = index
        log.info('--------------------GDAL_PLUGIN Add granule------------')
        super(GSAddMosaicGranule, self).__init__(*args, **kwargs)

    def execute(self, context):
        task_instance = context['task_instance']
        granule = task_instance.xcom_pull('rsync_' + str(self.index), key=xk.GRANULE_TO_UPLOAD_PREFIX + str(self.index))
        log.info("GSAddMosaicGranule params list")
        log.info('Mosaic granule: %s', granule)
        store = self.catalog.get_store(self.store_name)
        granule = 'file://' + self.mosaic_path + '/' + granule
        log.info(granule)
        self.catalog.harvest_externalgranule(granule, store)

def generate_wfs_dict(s2_product, GS_WORKSPACE, GS_LAYER):
    return {
        "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wfs",
        "method": "GET",
        "code": "GetFeature",
        "type": "application/json",
        "href": r"${BASE_URL}"+"/{}/ows?service=wfs&version=2.0.0&request=GetFeature&typeNames={}:{}&CQL_FILTER=eoIdentifier='{}'&outputFormat=application/json".format(
            GS_WORKSPACE,
            GS_WORKSPACE,
            GS_LAYER, 
            s2_product.manifest_safe_path.rsplit('.SAFE', 1)[0])
    }

def generate_wms_dict(GS_WORKSPACE, GS_LAYER, granule_coordinates, GS_WMS_WIDTH, GS_WMS_HEIGHT, GS_WMS_FORMAT, s2_product):
    bbox = str(granule_coordinates[0][3][0])+","+str(granule_coordinates[0][1][0])
    return {
        "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wms",
        "method": "GET", 
        "code": "GetMap", 
        "type": "image/jpeg", 
        "href": r"${BASE_URL}"+"/{}/{}/ows?service=wms&request=GetMap&version=1.3.0&LAYERS={}&BBOX={}&WIDTH={}&HEIGHT={}&FORMAT=image/jpeg&CQL_FILTER=eoIdentifier='{}'".format(
            GS_WORKSPACE, 
            GS_LAYER, 
            GS_LAYER, 
            bbox.strip(), 
            GS_WMS_WIDTH, 
            GS_WMS_HEIGHT, 
            s2_product.manifest_safe_path.rsplit('.SAFE', 1)[0])
    }

def generate_wcs_dict(granule_coordinates, GS_WORKSPACE, s2_product, coverage_id):
    long1, long2 = (float(granule_coordinates[0][3][0].split(",")[0]),float(granule_coordinates[0][1][0].split(",")[0]))
    lat1, lat2 = (float(granule_coordinates[0][3][0].split(",")[1]),float(granule_coordinates[0][1][0].split(",")[1]))
    return {
        "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wcs",
        "method": "GET",
        "code": "GetCoverage",
        "type": "image/jp2",
        "href": r"${BASE_URL}"+"/{}/wcs?service=WCS&version=2.0.1&coverageId={}&request=GetCoverage&format=jpeg2000&subset=http://www.opengis.net/def/axis/OGC/0/Long({},{})&subset=http://www.opengis.net/def/axis/OGC/0/Lat({},{})&scaleaxes=i(0.1),j(0.1)&CQL_FILTER=eoIdentifier='{}'".format(
            GS_WORKSPACE, 
            coverage_id, 
            str(long1).strip(), 
            str(long2).strip(), 
            str(lat1).strip(), 
            str(lat2).strip(), 
            s2_product.manifest_safe_path.rsplit('.SAFE', 1)[0])
    }

def publish_product(geoserver_username, geoserver_password, geoserver_rest_endpoint, get_inputs_from, *args, **kwargs):
    # Pull Zip path from XCom
    log.info("publish_product task")
    log.info("""
        geoserver_username: {}
        geoserver_password: ******
        geoserver_rest_endpoint: {}
        """.format(
            geoserver_username,
            geoserver_rest_endpoint
        )
    )
    task_instance = kwargs['ti']

    zip_files=list()
    if get_inputs_from != None:
        log.info("Getting inputs from: " +get_inputs_from)
        zip_files = task_instance.xcom_pull(task_ids=get_inputs_from, key=XCOM_RETURN_KEY)
    else:
        log.info("Getting inputs from: product_zip_task" )
        zip_files = task_instance.xcom_pull('product_zip_task', key='product_zip_paths')
    
    log.info("zip_file_paths: {}".format(zip_files))
    if zip_files is not None:
        published_ids=list()
        for zip_file in zip_files:
            # POST product.zip
            d = open(zip_file, 'rb').read()
            a = requests.auth.HTTPBasicAuth(geoserver_username, geoserver_password)
            h = {'Content-type': 'application/zip'}

            r = requests.post(geoserver_rest_endpoint,
                auth=a,
                data=d,
                headers=h)

            if r.status_code == 201:
                published_ids.append(r.text)
                log.info("Successfully published product '{}' (HTTP {})".format(r.text,r.status_code))
            elif r.status_code == 500:
                log.warn("Error during publishing product '{}' (HTTP {}: {})".format(zip_file, r.status_code, r.text))
            else:
                log.warn("Unknown response during publishing '{}' (HTTP {}: {})".format(zip_file, r.status_code, r.text))
        return published_ids
    else:
        log.warn("No product.zip found.")

class GDALPlugin(AirflowPlugin):
    name = "GeoServer_plugin"
    operators = [GSAddMosaicGranule]

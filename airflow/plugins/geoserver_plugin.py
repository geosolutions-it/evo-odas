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

def generate_wfs_cap_dict(product_identifier, gs_workspace, gs_featuretype, gs_wfs_version):
    return {
        "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wfs",
        "method": "GET",
        "code": "GetCapabilities",
        "type": "application/xml",
        "href": "${BASE_URL}"+"/{}/{}/ows?service=WFS&request=GetCapabilities&version={}&CQL_FILTER=eoIdentifier='{}'".format(
            gs_workspace,
            gs_featuretype,
            gs_wfs_version,
            product_identifier
        )
    }

def generate_wms_cap_dict(product_identifier, gs_workspace, gs_layer, gs_wms_version):
    return {
        "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wms",
        "method": "GET",
        "code": "GetCapabilities",
        "type": "application/xml",
        "href": "${BASE_URL}" + "/{}/{}/ows?service=WMS&request={}&version={}&CQL_FILTER=eoIdentifier='{}'".format(
            gs_workspace,
            gs_layer,
            "GetCapabilities",
            gs_wms_version,
            product_identifier
        )
    }

def generate_wcs_cap_dict(product_identifier, gs_workspace, gs_layer, gs_wcs_version):
    return {
            "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wcs",
            "method": "GET",
            "code": "GetCapabilities",
            "type": "application/xml",
            "href": "${BASE_URL}"+"/{}/{}/ows?service=WCS&request=GetCapabilities&version={}&CQL_FILTER=eoParentIdentifier='{}'".format(
                gs_workspace,
                gs_layer,
                gs_wcs_version,
                product_identifier
                )
            }


def generate_wfs_dict(product_identifier, gs_workspace, gs_featuretype, gs_wfs_format, gs_wfs_version):
    return {
        "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wfs",
        "method": "GET",
        "code": "GetFeature",
        "type": "application/json",
        "href": r"${BASE_URL}"+"/{}/ows?service=wfs&version={}&request=GetFeature&typeNames={}:{}&CQL_FILTER=eoIdentifier='{}'&outputFormat={}".format(
            gs_workspace,
            gs_wfs_version,
            gs_workspace,
            gs_featuretype,
            product_identifier,
            gs_wfs_format
        )
    }

def generate_wms_dict(product_identifier, gs_workspace, gs_layer, bbox, gs_wms_width, gs_wms_height, gs_wms_format, gs_wms_version):
    return {
        "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wms",
        "method": "GET",
        "code": "GetMap",
        "type": gs_wms_format,
        "href": r"${BASE_URL}"+"/{}/{}/ows?service=wms&request=GetMap&version={}&LAYERS={}&BBOX={},{},{},{}&WIDTH={}&HEIGHT={}&FORMAT={}&CQL_FILTER=eoIdentifier='{}'".format(
            gs_workspace,
            gs_layer,
            gs_wms_version,
            gs_layer,
            bbox["long_min"],
            bbox["lat_min"],
            bbox["long_max"],
            bbox["lat_max"],
            gs_wms_width,
            gs_wms_height,
            gs_wms_format,
            product_identifier
        )
    }

def generate_wcs_dict(product_identifier, bbox, gs_workspace, coverage_id, gs_wcs_scale_i, gs_wcs_scale_j, gs_wcs_format, gs_wcs_version):
    return {
        "offering": "http://www.opengis.net/spec/owc-atom/1.0/req/wcs",
        "method": "GET",
        "code": "GetCoverage",
        "type": gs_wcs_format,
        "href": r"${BASE_URL}"+"/{}/{}/wcs?service=WCS&version={}&coverageId={}&request=GetCoverage&format={}&subset=http://www.opengis.net/def/axis/OGC/0/Long({},{})&subset=http://www.opengis.net/def/axis/OGC/0/Lat({},{})&scaleaxes=i({}),j({})&CQL_FILTER=eoIdentifier='{}'".format(
            gs_workspace,
            coverage_id,
            gs_wcs_version,
            str(gs_workspace+"__"+coverage_id),
            gs_wcs_format,
            bbox["long_min"],
            bbox["lat_min"],
            bbox["long_max"],
            bbox["lat_max"],
            gs_wcs_scale_i,
            gs_wcs_scale_j,
            product_identifier
        )
    }

def create_owslinks_dict(
        product_identifier,
        granule_bbox,
        gs_workspace,
        gs_wms_layer,
        gs_wms_width,
        gs_wms_height,
        gs_wms_format,
        gs_wms_version,
        gs_wfs_featuretype,
        gs_wfs_format,
        gs_wfs_version,
        gs_wcs_layer,
        gs_wcs_coverage_id,
        gs_wcs_scale_i,
        gs_wcs_scale_j,
        gs_wcs_format,
        gs_wcs_version,
    ):
    dict = {}
    links = []

    links.append(
        generate_wms_cap_dict(
            product_identifier=product_identifier,
            gs_workspace=gs_workspace,
            gs_layer=gs_wms_layer,
            gs_wms_version=gs_wms_version
        )
    )
    links.append(
        generate_wfs_cap_dict(
            product_identifier=product_identifier,
            gs_workspace=gs_workspace,
            gs_featuretype=gs_wfs_featuretype,
            gs_wfs_version=gs_wfs_version
        )
    )
    links.append(
        generate_wcs_cap_dict(
            product_identifier=product_identifier,
            gs_workspace=gs_workspace,
            gs_layer=gs_wcs_layer,
            gs_wcs_version=gs_wcs_version
        )
    )
    links.append(
        generate_wms_dict(
            product_identifier=product_identifier,
            gs_workspace=gs_workspace,
            gs_layer=gs_wms_layer,
            bbox=granule_bbox,
            gs_wms_width=gs_wms_width,
            gs_wms_height=gs_wms_height,
            gs_wms_format=gs_wms_format,
            gs_wms_version=gs_wms_version
        )
    )
    links.append(
        generate_wfs_dict(
            product_identifier=product_identifier,
            gs_workspace=gs_workspace,
            gs_featuretype=gs_wfs_featuretype,
            gs_wfs_format=gs_wfs_format,
            gs_wfs_version = gs_wfs_version
        )
    )
    links.append(
        generate_wcs_dict(
            product_identifier=product_identifier,
            bbox=granule_bbox,
            gs_workspace=gs_workspace,
            coverage_id=gs_wcs_coverage_id,
            gs_wcs_format=gs_wcs_format,
            gs_wcs_scale_i=gs_wcs_scale_i,
            gs_wcs_scale_j=gs_wcs_scale_j,
            gs_wcs_version=gs_wcs_version
        )
    )
    dict["links"] = links

    return dict

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
            log.info("Publishing: {}".format(zip_file))
            d = open(zip_file, 'rb').read()
            a = requests.auth.HTTPBasicAuth(geoserver_username, geoserver_password)
            h = {'Content-type': 'application/zip'}

            r = requests.post(geoserver_rest_endpoint,
                auth=a,
                data=d,
                headers=h)

            if r.ok:
                published_ids.append(r.text)
                log.info("Successfully published product '{}' (HTTP {})".format(r.text,r.status_code))
            else:
                log.warn("Error during publishing product '{}' (HTTP {}: {})".format(zip_file, r.status_code, r.text))
		r.raise_for_status()
        return published_ids
    else:
        log.warn("No product.zip found.")

class GDALPlugin(AirflowPlugin):
    name = "GeoServer_plugin"
    operators = [GSAddMosaicGranule]

import os
import logging

from airflow.operators import BaseOperator
from airflow.operators import PythonOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import XCOM_RETURN_KEY
import config.xcom_keys as xk
#from config import xcom_keys as xk
from geoserver_plugin import create_owslinks_dict

try:
    from osgeo import gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules, install gdal with python bindings')


log = logging.getLogger(__name__)

import sys
import os
import logging
import pprint
import json
from shutil import copyfile
from zipfile import ZipFile
from S1Reader import S1GDALReader
from utils import TemplatesResolver

log = logging.getLogger(__name__)

def create_search_dict(metadata, originalPackageLocation):
    return \
        {  # USED IN METADATA TEMPLATE and as SEARCH PARAMETERS
            "type": "Feature",
            "geometry": {
                "type": metadata['footprint']['type'],
                "coordinates": metadata['footprint']['coordinates']
            },
            "properties": {
                "eop:identifier": metadata['NAME'],
                'timeStart': metadata['ACQUISITION_START_TIME'],
                'timeEnd': metadata['ACQUISITION_STOP_TIME'],
                "originalPackageLocation": originalPackageLocation,
                "thumbnailURL": None,
                "quicklookURL": None,
                "eop:parentIdentifier": "SENTINEL1",
                "eop:productionStatus": None,
                "eop:acquisitionType": None,
                'eop:orbitNumber': metadata['ORBIT_NUMBER'],
                'eop:orbitDirection': metadata['ORBIT_DIRECTION'],
                "eop:track": None,
                "eop:frame": None,
                "eop:swathIdentifier": metadata['SWATH'],
                "opt:cloudCover": None,
                "opt:snowCover": None,
                "eop:productQualityStatus": None,
                "eop:productQualityDegradationStatus": None,
                "eop:processorName": None,
                "eop:processingCenter": metadata['FACILITY_IDENTIFIER'],  # ?
                "eop:creationDate": None,
                "eop:modificationDate": None,
                "eop:processingDate": None,
                "eop:sensorMode": metadata['BEAM_MODE'],
                "eop:archivingCenter": None,
                "eop:processingMode": None,
                "eop:availabilityTime": None,
                "eop:acquisitionStation": None,
                "eop:acquisitionSubtype": None,
                "eop:startTimeFromAscendingNode": None,
                "eop:completionTimeFromAscendingNode": None,
                "eop:illuminationAzimuthAngle": None,
                "eop:illuminationZenithAngle": None,
                "eop:illuminationElevationAngle": None,
                "sar:polarisationMode": None,
                "sar:polarisationChannels": None,
                "sar:antennaLookDirection": None,
                "sar:minimumIncidenceAngle": None,
                "sar:maximumIncidenceAngle": None,
                "sar:dopplerFrequency": None,
                "sar:incidenceAngleVariation": None,
                "eop:resolution": None,
            }
      }

def create_metadata_dict(metadata):
    return \
        {
            # USED IN METADATA TEMPLATE ONLY
            'eoProcessingLevel': "L1",
            'eoSensorType': "RADAR",
            'eoProductType': metadata['PRODUCT_TYPE'],
            'eoInstrument': metadata['SENSOR_IDENTIFIER'],
            'eoPlatform': metadata['SATELLITE_IDENTIFIER'],
            'eoPlatformSerialIdentifier': metadata['MISSION_ID'],
        }

def create_description_dict(metadata, originalPackageLocation):
        return \
        {
            # TO BE USED IN THE PRODUCT ABSTRACT TEMPLATE
            'timeStart': metadata['ACQUISITION_START_TIME'],
            'timeEnd': metadata['ACQUISITION_STOP_TIME'],
            'originalPackageLocation': originalPackageLocation
        }

def create_product_description(description_dict):
    tr = TemplatesResolver()
    html_description = tr.generate_product_abstract(description_dict)
    return html_description

def create_product_metadata(metadata_dict):
    tr = TemplatesResolver()
    metadata_xml = tr.generate_sentinel1_product_metadata(metadata_dict)
    return metadata_xml

def create_procuct_zip(
                       processing_dir,
                       search_params_dict,
                       metadata_xml,
                       description_html,
                       thumbnail_path,
                       granules_dict,
                       owslinks_dict,
                    ):

    files = []

    path = os.path.join(processing_dir, "description.html")
    with open(path, "w") as f:
        f.write(description_html)
    files.append(path)


    path = os.path.join(processing_dir, "metadata.xml")
    with open(path, "w") as f:
        f.write(metadata_xml)
    files.append(path)

    files.append(thumbnail_path)

    # create granules.json
    log.info("Creating granules.json")

    log.info("granules_dict: \n{}".format(pprint.pformat(granules_dict)))
    path = os.path.join(processing_dir, 'granules.json')
    with open(path, 'w') as f:
        json.dump(granules_dict, f, indent=4)
    files.append(path)

    # Use the coordinates from the granules in the product.json as the s1reader seems to
    # swap the footprint coordinates, see https://github.com/geosolutions-it/evo-odas/issues/192
    granule_coords = granules_dict.get('features')[0].get('geometry').get('coordinates')
    search_params_dict['geometry']['coordinates'] = granule_coords

    # dump product.json to file
    log.info("Creating product.json")
    path = os.path.join(processing_dir, 'product.json')
    with open(path, 'w') as f:
        json.dump(search_params_dict, f, indent=4)
    files.append(path)

    # create owslinks.json and dump it to file
    log.info("Creating owslinks.json")
    path = os.path.join(processing_dir, 'owslinks.json')
    with open(path, 'w') as f:
        json.dump(owslinks_dict, f, indent=4)
    files.append(path)

    # create product.zip
    log.info("Creating product zip with files: " + pprint.pformat(files))
    path = os.path.join(processing_dir, 'product.zip')
    try:
        with ZipFile(path, 'w') as z:
            for f in files:
                z.write(f, os.path.basename(f))
    except:
        log.error("failed to create product.zip")

    # cleanup
    for file in files:
        os.remove(file)

    return path


def get_bbox_from_granule(granule_path):
    datastore = gdal.Open(granule_path)
    ulx, xres, xskew, uly, yskew, yres = datastore.GetGeoTransform()
    lrx = ulx + (datastore.RasterXSize * xres)
    lry = uly + (datastore.RasterYSize * yres)
    llx, lly = (ulx, lry)
    urx, ury = (lrx, uly)
    return [ [ulx,uly], [llx,lly], [lrx,lry], [urx,ury], [ulx,uly] ]

def collect_granules_metadata(granules_paths, granules_upload_dir, bands_dict):
    # create granules.json
    granules_dict = {
        "type": "FeatureCollection",
        "features": []
    }
    log.info("Collecting granules metadata. Number of granules to process: {}".format(len(granules_paths)))
    for granule in granules_paths:
        granule_name = os.path.basename(granule)
        location = os.path.join(granules_upload_dir, granule_name)
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": None
            },
            "properties": {
                "location": location,
                "band"    : bands_dict[granule_name.split("-")[3]]
            }
        }
        datastore = gdal.Open(granule)
        metadata_dict = datastore.GetMetadata()
        log.info(pprint.pformat(metadata_dict))

        # list of list of lists
        coordinates = []
        bbox_polygon = get_bbox_from_granule(granule)
        log.info("bbox_str: " + pprint.pformat(bbox_polygon))
        coordinates.append(bbox_polygon)
        feature['geometry']['coordinates'] = coordinates
        log.info("COORDINATES: " + pprint.pformat(coordinates))
        granules_dict['features'].append(feature)

        lat_lower_left, lat_upper_right = (bbox_polygon[1][1], bbox_polygon[3][1])
        long_lower_left, long_upper_right = (bbox_polygon[1][0], bbox_polygon[3][0])
        long_min = min(long_lower_left, long_upper_right)
        long_max = max(long_lower_left, long_upper_right)
        lat_min = min(lat_lower_left, lat_upper_right)
        lat_max = max(lat_lower_left, lat_upper_right)
        bbox = {
            "long_min": long_min,
            "long_max": long_max,
            "lat_min": lat_min,
            "lat_max": lat_max,

        }
    return (granules_dict, bbox)

class S1MetadataOperator(BaseOperator):
    """ S1MetadataOperator is an abstract level for generating the S1 metadata files and adding it to product.zip later on. It calls another python_callable create_procuct_zip which calls S1 utils to generate meta-data files one by one

    Args:
        granules_paths (list): carrying paths to granules
        granules_upload_dir (str): path to the upload directory
        processing_dir (str): path to the processing directory
        get_inputs_from (str): task_ids used to fetch input files from XCom 

    Returns:
        list: list of output product.zip paths
    """

    @apply_defaults
    def __init__(self,
                 granules_paths,
                 granules_upload_dir,
                 processing_dir,
                 bands_dict,
                 original_package_download_base_url,
                 gs_workspace,
                 gs_wms_layer,
                 gs_wms_width,
                 gs_wms_height,
                 gs_wms_format,
                 gs_wms_version,
                 gs_wfs_featuretype,
                 gs_wfs_format,
                 gs_wfs_version,
                 gs_wcs_coverage_id,
                 gs_wcs_scale_i,
                 gs_wcs_scale_j,
                 gs_wcs_format,
                 gs_wcs_version,
                 get_inputs_from=None,
                 *args, **kwargs):
        self.granules_paths = granules_paths
        self.granules_upload_dir = granules_upload_dir
        self.processing_dir = processing_dir
        self.bands_dict = bands_dict
        self.gs_workspace = gs_workspace
        self.gs_wms_layer = gs_wms_layer
        self.gs_wms_width = gs_wms_width
        self.gs_wms_height = gs_wms_height
        self.gs_wms_format = gs_wms_format
        self.gs_wms_version = gs_wms_version
        self.gs_wfs_featuretype = gs_wfs_featuretype
        self.gs_wfs_format = gs_wfs_format
        self.gs_wfs_version = gs_wfs_version
        self.gs_wcs_coverage_id = gs_wcs_coverage_id
        self.gs_wcs_scale_i = gs_wcs_scale_i
        self.gs_wcs_scale_j = gs_wcs_scale_j
        self.gs_wcs_format = gs_wcs_format
        self.gs_wcs_version = gs_wcs_version
        self.original_package_download_base_url = original_package_download_base_url

        self.get_inputs_from = get_inputs_from

        super(S1MetadataOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('--------------------S1Metadata_PLUGIN running------------')
        task_instance = context['task_instance']

        log.info("Receiving from 'get_input_from':\n{}".format(self.get_inputs_from))

        download_task_id = self.get_inputs_from['download_task_id']
        addo_task_ids = self.get_inputs_from['addo_task_ids']
        upload_task_ids = self.get_inputs_from['upload_task_ids']
        archive_product_task_id = self.get_inputs_from['archive_product_task_id']

        downloaded = context['task_instance'].xcom_pull(task_ids=download_task_id, key=XCOM_RETURN_KEY)

        local_granules_paths = []
        for tid in addo_task_ids:
            local_granules_path = context['task_instance'].xcom_pull(task_ids=tid, key=XCOM_RETURN_KEY)
            if local_granules_path:
                local_granules_paths +=  local_granules_path
        uploaded_granules_paths = context['task_instance'].xcom_pull(task_ids=upload_task_ids, key=XCOM_RETURN_KEY)
        original_package_path = context['task_instance'].xcom_pull(task_ids=archive_product_task_id, key=XCOM_RETURN_KEY)
        granules_dict, bbox = collect_granules_metadata(local_granules_paths, self.granules_upload_dir, self.bands_dict)

        if not downloaded:
            log.info("No products from Download task, Nothing to do.")
            return list()
        if not local_granules_paths:
            log.info("No local granules from processing, Nothing to do.")
            return list()
        if not uploaded_granules_paths:
            log.info("No uploaded granules from upload task, Nothing to do.")
            return list()
        if not original_package_path:
            log.info("No original package path from original package upload task, Nothing to do.")
            return list()

        safe_package_path = downloaded.keys()[0]
        safe_package_filename = os.path.basename(safe_package_path)
        product_id = downloaded[safe_package_path].get('title')
        originalPackageLocation = self.original_package_download_base_url + safe_package_filename
        processing_dir = os.path.join(self.processing_dir, product_id)
        if not os.path.exists(processing_dir):
            os.makedirs(processing_dir)

        log.info('safe_package_path: {}'.format(safe_package_path))
        log.info('local_granules_paths: {}'.format(local_granules_paths))

        s1reader = S1GDALReader(safe_package_path)
        product_metadata = s1reader.get_metadata()
        product_metadata['footprint'] = s1reader.get_footprint()
        log.info(pprint.pformat(product_metadata, indent=4))

        timeStart = product_metadata['ACQUISITION_START_TIME']
        timeEnd = product_metadata['ACQUISITION_STOP_TIME']

        owslinks_dict = create_owslinks_dict(
            product_identifier=product_id,
            timestart= timeStart,
            timeend = timeEnd,
            granule_bbox=bbox,
            gs_workspace=self.gs_workspace,
            gs_wms_layer=self.gs_wms_layer,
            gs_wms_width=self.gs_wms_width,
            gs_wms_height=self.gs_wms_height,
            gs_wms_format=self.gs_wms_format,
            gs_wms_version=self.gs_wms_version,
            gs_wfs_featuretype=self.gs_wfs_featuretype,
            gs_wfs_format=self.gs_wfs_format,
            gs_wfs_version=self.gs_wfs_version,
            gs_wcs_coverage_id=self.gs_wcs_coverage_id,
            gs_wcs_scale_i=self.gs_wcs_scale_i,
            gs_wcs_scale_j=self.gs_wcs_scale_j,
            gs_wcs_format=self.gs_wcs_format,
            gs_wcs_version=self.gs_wcs_version
        )

        # create thumbnail
        # TODO: create proper thumbnail from quicklook. Also remove temp file
        log.info("Creating thumbnail")
        thumbnail_path = os.path.join(processing_dir, "thumbnail.png")
        quicklook_path = s1reader.get_quicklook()
        log.info(pprint.pformat(quicklook_path))
        copyfile(quicklook_path, thumbnail_path)

        search_params_dict = create_search_dict(product_metadata, originalPackageLocation)
        log.info(pprint.pformat(search_params_dict))

        metadata_dict = create_metadata_dict(product_metadata)
        log.info(pprint.pformat(metadata_dict))

        description_dict = create_description_dict(product_metadata, originalPackageLocation)
        log.info(pprint.pformat(description_dict))

        # create description.html and dump it to file
        log.info("Creating description.html")
        html_description = create_product_description(description_dict)
        search_params_dict['htmlDescription'] = html_description

        # create metadata XML
        log.info("Creating metadata.xml")
        metadata_xml = create_product_metadata(metadata_dict)

        po = PythonOperator(
            task_id="s1_metadata_dictionary_creation",
            python_callable=create_procuct_zip,
            op_kwargs={
                'processing_dir': processing_dir,
                'search_params_dict' : search_params_dict,
                'description_html': html_description,
                'metadata_xml': metadata_xml,
                'granules_dict': granules_dict,
                'owslinks_dict' : owslinks_dict,
                'thumbnail_path' : thumbnail_path
            }
        )


        out = po.execute(context)
        zip_paths = list()
        if out:
            zip_paths.append(out)
        return zip_paths


class Sentinel1Plugin(AirflowPlugin):
    name = "RSYNC_plugin"
    operators = [S1MetadataOperator]

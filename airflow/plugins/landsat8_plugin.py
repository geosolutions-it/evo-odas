from collections import namedtuple
import json
import logging
import os
import re
import pprint
import shutil
import zipfile
import gzip
import pprint
from datetime import timedelta
import psycopg2
import urllib
import requests

from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import XCOM_RETURN_KEY
from utils import TemplatesResolver
from pgmagick import Image
from osgeo import osr

from geoserver_plugin import create_owslinks_dict
from geoserver_plugin import is_product_published

log = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)

BAND_NAMES = {
    "B1": "B1",
    "B2": "B2",
    "B3": "B3",
    "B4": "B4",
    "B5": "B5",
    "B6": "B6",
    "B7": "B7",
    "B8": "B8",
    "B9": "B9",
    "B10": "B10",
    "B11": "B11",
    "B12": "B12",
}

BoundingBox = namedtuple("BoundingBox", [
    "ullon",
    "ullat",
    "urlon",
    "urlat",
    "lllon",
    "lllat",
    "lrlon",
    "lrlat"
])

def download_file(url, destination_directory):
    response = requests.get(url, stream=True)
    full_path = os.path.join(destination_directory, url.rpartition("/")[-1])
    with open(full_path, "wb") as fh:
        for chunk in response.iter_content(chunk_size=1024):
            fh.write(chunk)
    return full_path


class DownloadSceneList(BaseOperator):

    @apply_defaults
    def __init__(self, download_dir, download_url, *args, **kwargs):
        super(DownloadSceneList, self).__init__(*args, **kwargs)
        self.download_url = download_url
        self.download_dir = download_dir

    def execute(self, context):
        try:
            os.makedirs(self.download_dir)
        except OSError as exc:
            if exc.errno == 17:
                pass  # directory already exists
            else:
                raise

        logger.info("Downloading {!r}...".format(self.download_url))
        download_file(self.download_url, self.download_dir)
        logger.info("Done!")


class ExtractSceneList(BaseOperator):

    @apply_defaults
    def __init__(self, download_dir, download_url, *args, **kwargs):
        super(ExtractSceneList, self).__init__(*args, **kwargs)
        self.download_dir = download_dir
        self.download_url = download_url

    def execute(self, context):
        path_to_extract = os.path.join(
            self.download_dir,
            self.download_url.rpartition("/")[-1]
        )
        target_path = "{}.csv".format(
            os.path.splitext(path_to_extract)[0])
        logger.info("Extracting {!r} to {!r}...".format(
            path_to_extract, target_path))
        with gzip.open(path_to_extract, 'rb') as zipped_fh, \
             open(target_path, "wb") as extracted_fh:
            extracted_fh.write(zipped_fh.read())


class UpdateSceneList(BaseOperator):

    @apply_defaults
    def __init__(self, download_dir, download_url, pg_dbname, pg_hostname,
                 pg_port, pg_username, pg_password,*args, **kwargs):
        super(UpdateSceneList, self).__init__(*args, **kwargs)
        self.download_dir = download_dir
        self.download_url = download_url
        self.pg_dbname = pg_dbname
        self.pg_hostname = pg_hostname
        self.pg_port = pg_port
        self.pg_username = pg_username
        self.pg_password = pg_password

    def execute(self, context):
        db_connection = psycopg2.connect(
            "dbname='{}' user='{}' host='{}' port='{}' password='{}'".format(
                self.pg_dbname, self.pg_username, self.pg_hostname, self.pg_port,
                self.pg_password
            )
        )
        logger.info("Deleting previous data from db...")
        with db_connection as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM scene_list;")
        filename = os.path.splitext(self.download_url.rpartition("/")[-1])[0]
        scene_list_path = os.path.join(
            self.download_dir,
            "{}.csv".format(filename))
        logger.info("Loading data from {!r} into db...".format(
            scene_list_path))
        with db_connection as conn, open(scene_list_path) as fh:
            fh.readline()
            with conn.cursor() as cursor:
                cursor.copy_from(fh, "scene_list", sep=",")
        return True


def create_original_package(get_inputs_from=None, files_list=None, out_dir=None, *args, **kwargs):
    # Pull Zip path from XCom
    log.info("create_original_package")
    log.info("""
        get_inputs_from: {}
        files_list: {}
        out_dir: {}
        """.format(
               get_inputs_from,
               files_list,
        out_dir
           )
           )

    task_instance = kwargs['ti']
    if get_inputs_from != None:
        log.info("Getting inputs from: " + pprint.pformat(get_inputs_from))
        # Searched products from Search Task (list of tuples)
        searched_prods = task_instance.xcom_pull(task_ids=get_inputs_from['search_task_id'], key=XCOM_RETURN_KEY)
        # Band TIFFs from download task
        files_list = task_instance.xcom_pull(task_ids=get_inputs_from['download_task_ids'], key=XCOM_RETURN_KEY)

    assert files_list is not None
    log.info("File List: {}".format(files_list))

    # Get Product ID from band file name
    '''
    filename=os.path.basename(files_list[0])
    m = re.match(r'(.*)_B.+\..+', filename)
    product_id = m.groups()[0]
    '''
    product_ids = list(tup[0] for tup in searched_prods)

    # ONLY HANDLE 1 PRODUCT AT A TIME
    product_id = product_ids[0]
    zipfile_path = os.path.join(out_dir, product_id + '.zip')
    with zipfile.ZipFile(zipfile_path, 'w') as myzip:
        for file in files_list:
            myzip.write(file, os.path.basename(file))

    return zipfile_path


def parse_mtl_data(buffer):
    """Parse input file-like object that contains metadata in MTL format."""
    metadata = {}
    current = metadata
    previous = metadata
    for line in buffer:
        key, value = (i.strip() for i in line.partition("=")[::2])
        if value == "L1_METADATA_FILE":
            pass
        elif key == "END":
            pass
        elif key == "GROUP":
            current[value] = {}
            previous = current
            current = current[value]
        elif key == "END_GROUP":
            current = previous
        elif key == "":
            pass
        else:
            try:
                parsed_value = int(value)
            except ValueError:
                try:
                    parsed_value = float(value)
                except ValueError:
                    parsed_value = str(value.replace('"', ""))
            current[key] = parsed_value
    return metadata


def get_bounding_box(product_metadata):
    return BoundingBox(
        ullon=float(product_metadata["CORNER_UL_LON_PRODUCT"]),
        ullat=float(product_metadata["CORNER_UL_LAT_PRODUCT"]),
        urlon=float(product_metadata["CORNER_UR_LON_PRODUCT"]),
        urlat=float(product_metadata["CORNER_UR_LAT_PRODUCT"]),
        lllon=float(product_metadata["CORNER_LL_LON_PRODUCT"]),
        lllat=float(product_metadata["CORNER_LL_LAT_PRODUCT"]),
        lrlon=float(product_metadata["CORNER_LR_LON_PRODUCT"]),
        lrlat=float(product_metadata["CORNER_LR_LAT_PRODUCT"]),
    )



def prepare_metadata(metadata, bounding_box, crs, original_package_location):

    date_acqired = metadata["PRODUCT_METADATA"]["DATE_ACQUIRED"]
    scene_center_time = metadata["PRODUCT_METADATA"]["SCENE_CENTER_TIME"]
    time_start_end = date_acqired + 'T' + scene_center_time.split('.')[0] + 'Z'

    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [bounding_box.ullon, bounding_box.ullat],
                [bounding_box.lllon, bounding_box.lllat],
                [bounding_box.lrlon, bounding_box.lrlat],
                [bounding_box.urlon, bounding_box.urlat],
                [bounding_box.ullon, bounding_box.ullat],
                ]],
            },
        "properties": {
            "eop:identifier": metadata[
                "METADATA_FILE_INFO"]["LANDSAT_PRODUCT_ID"],
            "timeStart": time_start_end,
            "timeEnd": time_start_end,
            "originalPackageLocation": original_package_location,
            "htmlDescription": None,
            "thumbnailURL": None,
            "quicklookURL": None,
            "crs": "EPSG:" + crs,
            "eop:parentIdentifier": "LANDSAT8",
            "eop:productionStatus": None,
            "eop:acquisitionType": None,
            "eop:orbitNumber": None,
            "eop:orbitDirection": None,
            "eop:track": None,
            "eop:frame": None,
            "eop:swathIdentifier": None,
            "opt:cloudCover": metadata["IMAGE_ATTRIBUTES"]["CLOUD_COVER"],
            "opt:snowCover": None,
            "eop:productQualityStatus": None,
            "eop:productQualityDegradationStatus": None,
            "eop:processorName": metadata[
                "METADATA_FILE_INFO"]["PROCESSING_SOFTWARE_VERSION"],
            "eop:processingCenter": None,
            "eop:creationDate": None,
            "eop:modificationDate": metadata[
                "METADATA_FILE_INFO"]["FILE_DATE"],
            "eop:processingDate": None,
            "eop:sensorMode": None,
            "eop:archivingCenter": None,
            "eop:processingMode": None,
            "eop:availabilityTime": None,
            "eop:acquisitionStation": metadata[
                "METADATA_FILE_INFO"]["STATION_ID"],
            "eop:acquisitionSubtype": None,
            "eop:startTimeFromAscendingNode": None,
            "eop:completionTimeFromAscendingNode": None,
            "eop:illuminationAzimuthAngle": metadata[
                "IMAGE_ATTRIBUTES"]["SUN_AZIMUTH"],
            "eop:illuminationZenithAngle": None,
            "eop:illuminationElevationAngle": metadata[
                "IMAGE_ATTRIBUTES"]["SUN_ELEVATION"],
            "eop:resolution": metadata[
                "PROJECTION_PARAMETERS"]["GRID_CELL_SIZE_REFLECTIVE"]
        }
    }


def prepare_granules(bounding_box, granule_paths):
    coordinates=[[
        [bounding_box.ullon, bounding_box.ullat],
        [bounding_box.lllon, bounding_box.lllat],
                        [bounding_box.lrlon, bounding_box.lrlat],
                        [bounding_box.urlon, bounding_box.urlat],
                        [bounding_box.ullon, bounding_box.ullat],
    ]]

    granules_dict = {
        "type": "FeatureCollection",
        "features": []
    }

    for i in range(len(granule_paths)):
        path = granule_paths[i]
        name_no_ext = os.path.splitext(os.path.basename(path))[0]
        band_name = name_no_ext.split('_')[-1:][0]
        log.info("Band Name: {} type: {}".format(band_name, type(band_name)))
        feature={
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": coordinates,
                },
            "properties": {
                "band": BAND_NAMES[band_name],
                "location": granule_paths[i]
                },
            "id": "GRANULE.{}".format(i+1)
        }
        granules_dict["features"].append(feature)

    return granules_dict

class Landsat8MTLReaderOperator(BaseOperator):
    """
    This class will read the .MTL file which is attached with the
    scene/product directory. The returned value from "final_metadata_dict"
    will be a python dictionary holding all the available keys from the
    .MTL file. this dictionary will be saved as json file to be added later
    to the product.zip. Also, the execute method will "xcom.push" the
    absolute path of the generated json file inside the context of the task

    Args:
        get_inputs_from (str): task_ids used to fetch input files from XCom
        metadata_xml_path (str): metadata.xml template path to be copied (temporarily)
        gs_workspace (str): GeoServer workspace to be used in OWSLinks.json
        gs_wfs_featuretype (str): GeoServer featuretype parameter to be used in OWSLinks.json
        gs_wfs_format (str): output format for WFS GetFeature request
        gs_wfs_version (str): WFS protocol version to use
        gs_wms_layer (str): GeoServer layer name to be used in OWSLinks.json
        gs_wms_width (int): WMS width to be used in OWSLinks.json
        gs_wms_height (int): WMS height to be used in OWSLinks.json
        gs_wms_format (str): WMS format to be used in OWSLinks.json
        gs_wms_version (str): WMS protocol version to use
        gs_wcs_scale_i (float): WCS I scale to be used in OWSLinks.json
        gs_wcs_scale_j (float): WCS J scale to be used in OWSLinks.json
        gs_wcs_format (str): WCS format to be used in OWSLinks.json
        gs_wcs_version (str): WCS protocol version to use

    Returns:
        tuple contains
        json_path (str): path of the generated product.json file
        granules_path (str): path of the generated granules.json file
        xml_template_path (str): path of the copied metadata.xml file
    """

    @apply_defaults
    def __init__(self, get_inputs_from,
                 metadata_xml_path,
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
                 gs_wcs_scale_i,
                 gs_wcs_scale_j,
                 gs_wcs_coverage_id,
                 gs_wcs_format,
                 gs_wcs_version,
                 *args, **kwargs):
        super(Landsat8MTLReaderOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from
        self.original_package_download_base_url = original_package_download_base_url
        self.metadata_xml_path = metadata_xml_path
        self.gs_workspace = gs_workspace
        self.gs_wms_layer = gs_wms_layer
        self.gs_wms_width = gs_wms_width
        self.gs_wms_height = gs_wms_height
        self.gs_wms_format = gs_wms_format
        self.gs_wms_version = gs_wms_version
        self.gs_wfs_featuretype = gs_wfs_featuretype
        self.gs_wfs_format = gs_wfs_format
        self.gs_wfs_version = gs_wfs_version
        self.gs_wcs_scale_i = gs_wcs_scale_i
        self.gs_wcs_scale_j = gs_wcs_scale_j
        self.gs_wcs_coverage_id = gs_wcs_coverage_id
        self.gs_wcs_format = gs_wcs_format
        self.gs_wcs_version = gs_wcs_version

    def execute(self, context):
        # fetch MTL file path from XCom
        mtl_path = context["task_instance"].xcom_pull(self.get_inputs_from["metadata_task_id"])
        if mtl_path is None:
            log.info("Nothing to process.")
            return
        # Uploaded granules paths from XCom
        upload_granules_task_ids = self.get_inputs_from["upload_task_ids"]
        granule_paths=[]
        for tid in upload_granules_task_ids:
            granule_paths += context["task_instance"].xcom_pull(tid)
        original_package_paths = context["task_instance"].xcom_pull(self.get_inputs_from["upload_original_package_task_id"])
        original_package_path = original_package_paths [0]
        original_package_filename = os.path.basename(original_package_path)
        original_package_location = self.original_package_download_base_url + original_package_filename
        product_id = os.path.splitext(original_package_filename)[0]
        # Get GDALInfo output from XCom
        gdalinfo_task_id = self.get_inputs_from["gdalinfo_task_id"]
        gdalinfo_dict = context["task_instance"].xcom_pull(gdalinfo_task_id)
        # Get GDALInfo output of one of the granules, CRS will be the same for all granules
        k = gdalinfo_dict.keys()[0]
        gdalinfo_out=gdalinfo_dict[k]
        # Extract projection WKT and get EPSG code
        match = re.findall(r'^(PROJCS.*]])', gdalinfo_out, re.MULTILINE | re.DOTALL)
        wkt_def = match[0]
        assert wkt_def is not None
        assert isinstance(wkt_def, basestring) or isinstance(wkt_def, str)
        sref = osr.SpatialReference()
        sref.ImportFromWkt(wkt_def)
        crs = sref.GetAttrValue("AUTHORITY",1)

        with open(mtl_path) as mtl_fh:
            parsed_metadata = parse_mtl_data(mtl_fh)
        bounding_box = get_bounding_box(parsed_metadata["PRODUCT_METADATA"])
        bbox_dict={
            "long_min": min(bounding_box.lllon, bounding_box.ullon),
            "lat_min" : min(bounding_box.lllat, bounding_box.lrlat),
            "long_max": min(bounding_box.lrlon, bounding_box.urlon),
            "lat_max" : min(bounding_box.ullat, bounding_box.urlat),
        }
        log.debug("BoundingBox: {}".format(pprint.pformat(bounding_box)))

        prepared_metadata = prepare_metadata(parsed_metadata, bounding_box, crs, original_package_location)
        timeStart, timeEnd = prepared_metadata['properties']['timeStart'], prepared_metadata['properties']['timeEnd']
        # create description.html and dump it to file
        log.info("Creating description.html")
        tr = TemplatesResolver()
        htmlAbstract = tr.generate_product_abstract({
            "timeStart" : timeStart,
            "timeEnd" : timeEnd,
            "originalPackageLocation" : original_package_location
        })
        log.debug(pprint.pformat(htmlAbstract))
        prepared_metadata["htmlDescription"] = htmlAbstract

        product_directory, mtl_name = os.path.split(mtl_path)
        granules_dict = prepare_granules(bounding_box, granule_paths)
        log.debug("Granules Dict: {}".format(pprint.pformat(granules_dict)))

        ows_links_dict = create_owslinks_dict(
            product_identifier = product_id,
            timestart= timeStart,
            timeend = timeEnd,
            granule_bbox= bbox_dict,
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
            gs_wcs_format = self.gs_wcs_format,
            gs_wcs_version=self.gs_wcs_version,
        )
        log.info("ows links: {}".format(pprint.pformat(ows_links_dict)))

        product_json_path = os.path.join(product_directory, "product.json")
        ows_links_path = os.path.join(product_directory, "owsLinks.json")
        granules_path = os.path.join(product_directory, "granules.json")
        xml_template_path = os.path.join(product_directory, "metadata.xml")
        description_html_path = os.path.join(product_directory, "description.html")

        # Create product.json
        with open(product_json_path, 'w') as out_json_fh:
            json.dump(prepared_metadata, out_json_fh, indent=4)
        # Create granules.json
        with open(granules_path, 'w') as out_granules_fh:
            json.dump(granules_dict, out_granules_fh, indent=4)
        # Create owsLinks.json
        with open(ows_links_path, 'w') as out_ows_links_fh:
            json.dump(ows_links_dict, out_ows_links_fh, indent=4)
        # Create metadata.xml
        shutil.copyfile(self.metadata_xml_path, xml_template_path)
        # Create description.html
        with open(description_html_path, "w") as out_description:
            out_description.write(htmlAbstract)

        return product_json_path, granules_path, ows_links_path, xml_template_path, description_html_path


class Landsat8ThumbnailOperator(BaseOperator):
    """This class will create a compressed, low resolution, square shaped
    thumbnail for the original scene then return the absolute path of the
    generated thumbnail

        Args:
            thumb_size_x (str): x dimension for the thumbnail size
            thumb_size_y (str): y dimension for the thumbnail size
            get_inputs_from (str): task_id used to fetch downloaded file from XCom

        Returns:
            output_path (str): path of the created thumbnail
    """

    @apply_defaults
    def __init__(self, get_inputs_from, thumb_size_x, thumb_size_y,
                 *args, **kwargs):
        super(Landsat8ThumbnailOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from
        self.thumb_size_x = thumb_size_x
        self.thumb_size_y = thumb_size_y

    def execute(self, context):
        downloaded_thumbnail = context["task_instance"].xcom_pull(
            self.get_inputs_from)
        if downloaded_thumbnail is None:
            log.info("Nothing to process.")
            return
        log.info("downloaded_thumbnail: {}".format(downloaded_thumbnail))
        img = Image(downloaded_thumbnail)
        least_dim = min(int(img.columns()), int(img.rows()))
        img.crop("{dim}x{dim}".format(dim=least_dim))
        img.scale(self.thumb_size_x+'x'+self.thumb_size_y)
        img.scale("{}x{}".format(self.thumb_size_x, self.thumb_size_y))
        output_path = os.path.join(
            os.path.dirname(downloaded_thumbnail), "thumbnail.jpeg")
        img.write(output_path)
        return output_path


class Landsat8ProductDescriptionOperator(BaseOperator):
    """Landsat8ProductDescriptionOperator will create a .html description file by copying it from
    its config path. (temporarily solution)

        Args:
            description_template (str): path to the template to be copied 
            download_dir (str): path to the destination directory

        Returns:
            output_path (str): path of the copied description template
    """
    @apply_defaults
    def __init__(self, description_template, download_dir, *args, **kwargs):
        super(Landsat8ProductDescriptionOperator, self).__init__(
            *args, **kwargs)
        self.description_template = description_template
        self.download_dir = download_dir

    def execute(self, context):
        if self.download_dir is None or self.description_template is None:
            log.info("Nothing to process.")
            return
        output_path = os.path.join(self.download_dir, "description.html")
        shutil.copyfile(self.description_template, output_path)
        return output_path


class Landsat8ProductZipFileOperator(BaseOperator):
    """This class will create product.zip file for landsat-8 scenes utilizing from the previous tasks

        Args:
            get_inputs_from (str): task_ids to get the generated metadata files
            output_dir (str): path to the destination directory

        Returns:
            output_paths (list): paths of the created product.zip files
    """

    @apply_defaults
    def __init__(self, get_inputs_from, output_dir, *args, **kwargs):
        super(Landsat8ProductZipFileOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from
        self.output_dir = output_dir

    def execute(self, context):
        if self.get_inputs_from is None or self.output_dir is None:
            log.info("Nothing to process.")
            return
        paths_to_zip = []
        for input_provider in self.get_inputs_from:
            inputs = context["task_instance"].xcom_pull(input_provider)
            if isinstance(inputs, basestring):
                paths_to_zip.append(inputs)
            else:  # the Landsat8MTLReaderOperator returns a tuple of strings
                paths_to_zip.extend(inputs)
        log.info("paths_to_zip: {}".format(paths_to_zip))
        output_path = os.path.join(self.output_dir, "product.zip")
        output_paths = [ output_path ]
        with zipfile.ZipFile(output_path, "w") as zip_handler:
            for path in paths_to_zip:
                zip_handler.write(path, os.path.basename(path))
        return output_paths


class Landsat8GranuleJsonFileOperator(BaseOperator):
    """Landsat8GranuleJsonFileOperator creates granule.json file for Landsat-8 scene/granule

        Args:
            location_prop (str): location property refers to the path of the granule on-disk to be added to the granule.json 

        Returns:
            True
    """

    @apply_defaults
    def __init__(self, location_prop, *args, **kwargs):
        self.location_prop = location_prop
        super(Landsat8GranuleJsonFileOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        return True


class Landsat8SearchOperator(BaseOperator):
    """Landsat8SearchOperator searches for scenes/granules to be downloaded from Landsat8DownloadOperator. It has search criteria (area of interest and cloud coverage). The current implementation is searching for granules in the created DB (from Landsat8_Scene_List DAG)

        Args:
            area (tuple): Named tuple instance contains name, path, row and bands info
            cloud_coverage (float): allowed cloud coverage percentage
            db_credentials (dict): carrying postgres connection string info
            startdate (str): date to start searching for scenes (acquisitiondate)
            enddate (str): end of date range/interval for searching scenes (acquisitiondate)
            filter_max (int): number to limit search results
            order_by (str): the column to use for ordering the returned results
            order_type (str): descending or ascending ordering

        Returns:
            tuple contains:
            product_id, entity_id, download_url
    """
    @apply_defaults
    def __init__(self, area, cloud_coverage, startdate, enddate, filter_max, order_by, order_type, db_credentials, *args, **kwargs):
        super(Landsat8SearchOperator, self).__init__(*args, **kwargs)
        self.area = area
        self.cloud_coverage = cloud_coverage
        self.startdate = startdate
        self.enddate = enddate
        self.filter_max = filter_max
        self.order_by = order_by
        self.order_type = order_type
        self.db_credentials = dict(db_credentials)

    def execute(self, context):
        if self.area is None or self.db_credentials is None:
            log.info("Either area of interest or credentials received with None.")
            return
        connection = psycopg2.connect(
            dbname=self.db_credentials["dbname"],
            user=self.db_credentials["username"],
            password=self.db_credentials["password"],
            host=self.db_credentials["hostname"],
            port=self.db_credentials["port"],
        )
        cursor = connection.cursor()
        data = (self.cloud_coverage, self.area.paths_rows, self.startdate, self.enddate)
        query = "SELECT productid, entityid, download_url FROM scene_list "

        self.conditions_list = []
        if self.cloud_coverage or self.area.path or self.area.row or self.startdate or self.enddate:
            where_stmt = " WHERE "
            query+=where_stmt

        if self.cloud_coverage:
            cloud_condition =  " cloudCover < %s "%(self.cloud_coverage)
            self.conditions_list.append(cloud_condition)
        else:
            cloud_condition = ''

        path_row_condition = ''
        if self.area.paths_rows:
            for path_row in self.area.paths_rows:
                path_row_condition +=  " path = %s AND row = %s OR "%(path_row[0],path_row[1])
            self.conditions_list.append(path_row_condition.strip(" OR "))
        else:
            path_row_condition = ''

        if self.startdate and self.enddate:
            startenddate_condition =  " acquisitiondate BETWEEN '%s' AND '%s' "%(self.startdate,self.enddate)
            self.conditions_list.append(startenddate_condition)
        else:
            startenddate_condition = ''

        if self.startdate and not self.enddate:
            startdate_condition =  " acquisitiondate > '%s' "%(self.startdate)
            self.conditions_list.append(startdate_condition)
        else:
            startdate_condition = ''

        if self.enddate and not self.startdate:
            enddate_condition =  " acquisitiondate < '%s' "%(self.enddate)
            self.conditions_list.append(enddate_condition)
        else:
            enddate_condition = ''

        conditions = ''
        for condition in self.conditions_list:
            conditions+= condition + " AND "

        query +=conditions.strip(" AND ")

        #kindly note that table name and sql keywords cannot be parametrized (e.g: using %s) so we had to use .format to order by
        query += " ORDER BY {} {} LIMIT {} ".format(self.order_by, self.order_type, self.filter_max)
        cursor.execute(query)
        search_results = cursor.fetchall()
        if search_results is None:
            log.error("Could not find any product for the {} area".format(self.area))
            return
        else:
            for record in search_results:
                log.info(
                    "Found {} product with {} scene id, available for download "
                   "through {} ".format(record[0], record[1], record[2]))
            return search_results


class Landsat8DownloadOperator(BaseOperator):
    """Landsat8DownloadOperator downloads scenes/granules which were found using Landsat8SearchOperator.

        Args:
            download_dir (str): path to the download directory
            get_inputs_from (str): task_id to pull the xcom value from search task
            url_fragment (str): string to be replaced with the filename(.tif/.mtl/.jpg)
            download_max (int): maximum number of products/scenes to be downloaded
            geoserver_username (str): account info to connect to Geoserver
            geoserver_password (str): account info to connect to Geoserver
            geoserver_rest_url (str): REST url of the geoserver

        Returns:
            target_path (str) : path to the downloaded Landsat-8 product/scene 
    """

    @apply_defaults
    def __init__(self, download_dir, get_inputs_from, url_fragment, download_max = None, geoserver_username = None, geoserver_password = None, geoserver_rest_url = None, geoserver_oseo_collection = None, download_timeout=timedelta(hours=1), *args, **kwargs):
        super(Landsat8DownloadOperator, self).__init__(
            execution_timeout=download_timeout, *args, **kwargs)
        self.download_dir = download_dir
        self.get_inputs_from = get_inputs_from
        self.url_fragment = url_fragment
        self.download_max = download_max
        self.geoserver_username = geoserver_username
        self.geoserver_password = geoserver_password
        self.geoserver_rest_url = geoserver_rest_url
        self.geoserver_oseo_collection = geoserver_oseo_collection

    def execute(self, context):
        task_inputs = context["task_instance"].xcom_pull(self.get_inputs_from)
        downloaded_products = []
        if task_inputs is None or len(task_inputs) == 0:
            log.info("Nothing to process.")
            return
        for scene in task_inputs:
            product_id, entity_id, download_url = scene
            product_published = is_product_published(self.geoserver_username, self.geoserver_password, self.geoserver_rest_url, collection_id = self.geoserver_oseo_collection, product_id=product_id)
            # in case the product was already published 
            if product_published:
                log.info("Found product {} already published. download operator will skip it".format(product_id))
                continue
            # in case the product wasn't published and still within download_max limit
            elif product_published == False and len(downloaded_products) < self.download_max:
                target_dir = os.path.join(self.download_dir, entity_id)               
                try:
                    os.makedirs(target_dir)
                except OSError as exc:
                    if exc.errno == 17:  # directory already exists
                        pass
                url = download_url.replace(
                    "index.html", "{}_{}".format(product_id, self.url_fragment))
                target_path = os.path.join(
                    target_dir,
                   "{}_{}".format(product_id, self.url_fragment)
                )
                try:
                    urllib.urlretrieve(url, target_path)
                    downloaded_products.append(target_path)
                except Exception:
                    log.exception(
                        msg="Error downloading {}".format(self.url_fragment))
                    raise
                else:
                    return target_path
            else:
                return

class LANDSAT8METADATAPlugin(AirflowPlugin):
    name = "landsat8_metadata_plugin"
    operators = [
        Landsat8MTLReaderOperator,
        Landsat8ThumbnailOperator,
        Landsat8ProductDescriptionOperator,
        Landsat8ProductZipFileOperator,
        Landsat8SearchOperator,
        Landsat8DownloadOperator,
        DownloadSceneList,
        ExtractSceneList,
        UpdateSceneList
    ]

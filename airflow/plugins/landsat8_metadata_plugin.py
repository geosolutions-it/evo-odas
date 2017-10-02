from collections import namedtuple
import json
import logging
import os
import pprint
import shutil
import zipfile

from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from pgmagick import Image

log = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)


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



def prepare_metadata(metadata, bounding_box):
    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [bounding_box.ullat, bounding_box.ullon],
                [bounding_box.urlat, bounding_box.urlon],
                [bounding_box.lllat, bounding_box.lllon],
                [bounding_box.lrlat, bounding_box.lrlon],
                [bounding_box.ullat, bounding_box.ullon],
            ]],
        },
        "properties": {
            "eop:identifier": metadata[
                "METADATA_FILE_INFO"]["LANDSAT_PRODUCT_ID"],
            "timeStart": metadata["PRODUCT_METADATA"]["SCENE_CENTER_TIME"],
            "timeEnd": metadata["PRODUCT_METADATA"]["SCENE_CENTER_TIME"],
            "originalPackageLocation": None,
            "thumbnailURL": None,
            "quicklookURL": None,
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


def prepare_granules(bounding_box, location):
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [bounding_box.ullat, bounding_box.ullon],
                        [bounding_box.urlat, bounding_box.urlon],
                        [bounding_box.lllat, bounding_box.lllon],
                        [bounding_box.lrlat, bounding_box.lrlon],
                        [bounding_box.ullat, bounding_box.ullon],
                    ]],
                },
                "properties": {"location": location},
                "id": "GRANULE.1"
            }
        ]
    }


class Landsat8MTLReaderOperator(BaseOperator):
    """
    This class will read the .MTL file which is attached with the
    scene/product directory.The returned value from "final_metadata_dict"
    will be a python dictionary holding all the available keys from the
    .MTL file. this dictionary will be saved as json file to be added later
    to the product.zip. Also, the execute method will "xcom.push" the
    absolute path of the generated json file inside the context of the task
    """

    @apply_defaults
    def __init__(self, get_inputs_from, loc_base_dir, metadata_xml_path,
                 *args, **kwargs):
        super(Landsat8MTLReaderOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from
        self.metadata_xml_path = metadata_xml_path
        self.loc_base_dir = loc_base_dir

    def execute(self, context):
        mtl_path = context["task_instance"].xcom_pull(self.get_inputs_from)
        with open(mtl_path) as mtl_fh:
            parsed_metadata = parse_mtl_data(mtl_fh)
        bounding_box = get_bounding_box(parsed_metadata["PRODUCT_METADATA"])
        prepared_metadata = prepare_metadata(parsed_metadata, bounding_box)
        product_directory, mtl_name = os.path.split(mtl_path)
        location = os.path.join(self.loc_base_dir, product_directory, mtl_name)
        granules_dict = prepare_granules(bounding_box, location)
        json_path = os.path.join(product_directory, "product.json")
        granules_path = os.path.join(product_directory, "granules.json")
        xml_template_path = os.path.join(product_directory, "metadata.xml")
        with open(json_path, 'w') as out_json_fh:
            json.dump(prepared_metadata, out_json_fh)
        with open(granules_path, 'w') as out_granules_fh:
            json.dump(granules_dict, out_granules_fh)
        shutil.copyfile(self.metadata_xml_path, xml_template_path)
        return json_path, granules_path, xml_template_path


class Landsat8ThumbnailOperator(BaseOperator):
    """This class will create a compressed, low resolution, square shaped
    thumbnail for the original scene then return the absolute path of the
    generated thumbnail
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
    """This class will create a .html description file by copying it from
    its config path
    """
    @apply_defaults
    def __init__(self, description_template, download_dir, *args, **kwargs):
        super(Landsat8ProductDescriptionOperator, self).__init__(
            *args, **kwargs)
        self.description_template = description_template
        self.download_dir = download_dir

    def execute(self, context):
        output_path = os.path.join(self.download_dir, "description.html")
        shutil.copyfile(self.description_template, output_path)
        return output_path


class Landsat8ProductZipFileOperator(BaseOperator):
    """This class will create product.zip file utilizing from the previous
    tasks
    """

    @apply_defaults
    def __init__(self, get_inputs_from, output_dir, *args, **kwargs):
        super(Landsat8ProductZipFileOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from
        self.output_dir = output_dir

    def execute(self, context):
        paths_to_zip = []
        for input_provider in self.get_inputs_from:
            inputs = context["task_instance"].xcom_pull(input_provider)
            if isinstance(inputs, basestring):
                paths_to_zip.append(inputs)
            else:  # the Landsat8MTLReaderOperator returns a tuple of strings
                paths_to_zip.extend(inputs)
        log.info("paths_to_zip: {}".format(paths_to_zip))
        output_path = os.path.join(self.output_dir, "product.zip")
        with zipfile.ZipFile(output_path, "w") as zip_handler:
            for path in paths_to_zip:
                zip_handler.write(path, os.path.basename(path))
        return output_path


class Landsat8GranuleJsonFileOperator(BaseOperator):

    @apply_defaults
    def __init__(self, location_prop, *args, **kwargs):
        self.location_prop = location_prop
        super(Landsat8GranuleJsonFileOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        return True


class LANDSAT8METADATAPlugin(AirflowPlugin):
    name = "landsat8_metadata_plugin"
    operators = [
        Landsat8MTLReaderOperator,
        Landsat8ThumbnailOperator,
        Landsat8ProductDescriptionOperator,
        Landsat8ProductZipFileOperator
    ]

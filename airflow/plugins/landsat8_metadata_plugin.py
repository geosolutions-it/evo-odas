import logging
import pprint
from datetime import datetime, timedelta
from airflow.operators import BaseOperator, BashOperator 
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import os, math, json, shutil, zipfile
from pgmagick import Image, Geometry
from jinja2 import Environment, FileSystemLoader, Template
#from config.workflow_settings import description_template
description_template = "./templates/product_description.html"

log = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)

''' This class will read the .MTL file which is attached with the scene/product directory.The returned value from "final_metadata_dict" will be a python dictionary holding all the available keys from the .MTL file. this dictionary will be saved as json file to be added later
to the product.zip. Also, the execute method will "xcom.push" the absolute path of the generated json file inside the context of the task
'''
class Landsat8MTLReaderOperator(BaseOperator):
        @apply_defaults
        def __init__(self, loc_base_dir, *args, **kwargs):
                self.loc_base_dir = loc_base_dir
                super(Landsat8MTLReaderOperator, self).__init__(*args, **kwargs)

        def execute(self, context):
                product_directory = context['task_instance'].xcom_pull('landsat8_translate_daraa_task', key='product_dir')
                log.info("PRODUCT DIRECTORY")
                log.info(product_directory)
                scene_files = os.listdir(product_directory)
                tiffs = []
                for item in scene_files:
                        if item.endswith("MTL.txt"):
                              lines = open(os.path.join(product_directory,item)).readlines()
                        if item.endswith("thumb_small.jpg"):
                              product_jpeg = os.path.join(product_directory,item)
                        if item.endswith(".TIF"):
                              tiffs.append(item)
                metadata_dictionary = {}
                for line in lines:
                        line_list = line.split("=")
                        metadata_dictionary[line_list[0].strip()] = line_list[1].strip() if len(line_list)>1 else "XXXXXXXXX"
                final_metadata_dict = {"type": "Feature", "geometry": {"type":"Polygon","coordinates":[[[float(metadata_dictionary["CORNER_UL_LAT_PRODUCT"]), float(metadata_dictionary["CORNER_UL_LON_PRODUCT"])],[float(metadata_dictionary["CORNER_UR_LAT_PRODUCT"]),float(metadata_dictionary["CORNER_UR_LON_PRODUCT"])],[float(metadata_dictionary["CORNER_LL_LAT_PRODUCT"]),float(metadata_dictionary["CORNER_LL_LON_PRODUCT"])],[float(metadata_dictionary["CORNER_LR_LAT_PRODUCT"]),float(metadata_dictionary["CORNER_LR_LON_PRODUCT"])],[float(metadata_dictionary["CORNER_UL_LAT_PRODUCT"]), float(metadata_dictionary["CORNER_UL_LON_PRODUCT"])]]]},
        "properties": {"eop:identifier" : metadata_dictionary["LANDSAT_PRODUCT_ID"][1:-1],
        "timeStart" : metadata_dictionary["SCENE_CENTER_TIME"], "timeEnd" : metadata_dictionary["SCENE_CENTER_TIME"], "originalPackageLocation" : None, "thumbnailURL" : None, "quicklookURL" : None,
        "eop:parentIdentifier" : None, "eop:productionStatus" : None, "eop:acquisitionType" : None, "eop:orbitNumber" : None,
        "eop:orbitDirection" : None, "eop:track" : None, "eop:frame" : None, "eop:swathIdentifier" : None, 
        "opt:cloudCover" : metadata_dictionary["CLOUD_COVER"],
        "opt:snowCover" : None,	"eop:productQualityStatus" : None, "eop:productQualityDegradationStatus" : None,
        "eop:processorName" : metadata_dictionary["PROCESSING_SOFTWARE_VERSION"][1:-1],
        "eop:processingCenter" : None, "eop:creationDate" : None,
        "eop:modificationDate" : metadata_dictionary["FILE_DATE"],
        "eop:processingDate" : None, "eop:sensorMode" : None, "eop:archivingCenter" : None, "eop:processingMode" : None,
        "eop:availabilityTime" : None,
        "eop:acquisitionStation" : metadata_dictionary["STATION_ID"][1:-1],
        "eop:acquisitionSubtype" : None, "eop:startTimeFromAscendingNode" : None, "eop:completionTimeFromAscendingNode" : None,
        "eop:illuminationAzimuthAngle" : metadata_dictionary["SUN_AZIMUTH"],
        "eop:illuminationZenithAngle" : None,
        "eop:illuminationElevationAngle" : metadata_dictionary["SUN_ELEVATION"],
        "eop:resolution" : metadata_dictionary["GRID_CELL_SIZE_REFLECTIVE"]}}

                with open(os.path.join(product_directory,'product.json'), 'w') as outfile:
                        json.dump(final_metadata_dict, outfile)
                log.info("######### JSON FILE PATH")
                log.info(os.path.abspath(os.path.join(product_directory,'product.json')))
                context['task_instance'].xcom_push(key='scene_time', value=metadata_dictionary["SCENE_CENTER_TIME"])
                context['task_instance'].xcom_push(key='product_json_abs_path', value=os.path.abspath(os.path.join(product_directory,'product.json')))
                context['task_instance'].xcom_push(key='product_jpeg_abs_path', value=product_jpeg)

                granules_dict = {"type": "FeatureCollection","features": [{"type": "Feature","geometry": {"type":"Polygon","coordinates":[[[float(metadata_dictionary["CORNER_UL_LAT_PRODUCT"]), float(metadata_dictionary["CORNER_UL_LON_PRODUCT"])],[float(metadata_dictionary["CORNER_UR_LAT_PRODUCT"]),float(metadata_dictionary["CORNER_UR_LON_PRODUCT"])],[float(metadata_dictionary["CORNER_LL_LAT_PRODUCT"]),float(metadata_dictionary["CORNER_LL_LON_PRODUCT"])],[float(metadata_dictionary["CORNER_LR_LAT_PRODUCT"]),float(metadata_dictionary["CORNER_LR_LON_PRODUCT"])],[float(metadata_dictionary["CORNER_UL_LAT_PRODUCT"]), float(metadata_dictionary["CORNER_UL_LON_PRODUCT"])]]]},"properties": {"location": os.path.join(self.loc_base_dir,product_directory,tiffs[0])},"id": "GRANULE.1"}]}

                with open(os.path.join(product_directory,'granules.json'), 'w') as outfile:
                        json.dump(granules_dict, outfile)
                log.info("######### JSON FILE PATH")
                log.info(os.path.abspath(os.path.join(product_directory,'granules.json')))
                context['task_instance'].xcom_push(key='granules_json_abs_path', value=os.path.abspath(os.path.join(product_directory,'granules.json')))

                shutil.copyfile("/home/moataz/geo-solutions-work/evo-odas/metadata-ingestion/test_data/metadata.xml",os.path.join(product_directory,"metadata.xml"))
                context['task_instance'].xcom_push(key='metadata_xml_abs_path', value=os.path.join(product_directory,"metadata.xml"))
                return True
# regarding class granules.json need to discuss about it, done and it will be having the 11 bands 


''' This class will create a compressed, low resolution, square shaped thumbnail for the 
original scene then return the absolute path of the generated thumbnail
'''
class Landsat8ThumbnailOperator(BaseOperator):

        @apply_defaults
        def __init__(self, thumb_size_x, thumb_size_y, *args, **kwargs):
                self.thumb_size_x = thumb_size_x
                self.thumb_size_y = thumb_size_y
                super(Landsat8ThumbnailOperator, self).__init__(*args, **kwargs)

        def execute(self, context):
                product_directory = context['task_instance'].xcom_pull('landsat8_translate_daraa_task', key='product_dir')
                jpeg_abs_path = context['task_instance'].xcom_pull('landsat8_product_json_task', key='product_jpeg_abs_path')
                img = Image(jpeg_abs_path)
                least_dim = min(int(img.columns()),int(img.rows()))
                img.crop(str(least_dim)+'x'+str(least_dim))
                img.scale(self.thumb_size_x+'x'+self.thumb_size_y)
                img.write(os.path.join(product_directory,"thumbnail.jpeg"))
                log.info(os.path.join(product_directory,"thumbnail.jpeg"))
                context['task_instance'].xcom_push(key='thumbnail_jpeg_abs_path', value=os.path.join(product_directory,"thumbnail.jpeg"))
                return True


''' This class will create a .html description file by copying it from its config path'''

class Landsat8ProductDescriptionOperator(BaseOperator):
        @apply_defaults
        def __init__(self, *args, **kwargs):
                super(Landsat8ProductDescriptionOperator, self).__init__(*args, **kwargs)

        def execute(self, context):
                product_desc_dict = {}
                product_directory = context['task_instance'].xcom_pull('landsat8_translate_daraa_task', key='product_dir')
                try:
                        shutil.copyfile(description_template, os.path.join(product_directory,"description.html"))
                except:
                        print "Couldn't find description.html"
                context['task_instance'].xcom_push(key='product_desc_abs_path', value=os.path.join(product_directory,"description.html"))
                return True

''' This class will create product.zip file utilizing from the previous tasks '''
class Landsat8ProductZipFileOperator(BaseOperator):
        @apply_defaults
        def __init__(self, *args, **kwargs):
                #self.zip_location = zip_location
                super(Landsat8ProductZipFileOperator, self).__init__(*args, **kwargs)

        def execute(self, context):
                product_directory = context['task_instance'].xcom_pull('landsat8_translate_daraa_task', key='product_dir')

                product_json_abs_path = context['task_instance'].xcom_pull('landsat8_product_json_task', key='product_json_abs_path')
                thumbnail_jpeg_abs_path = context['task_instance'].xcom_pull('landsat8_product_thumbnail_task', key='thumbnail_jpeg_abs_path')
                product_desc_abs_path = context['task_instance'].xcom_pull('landsat8_product_description_task', key='product_desc_abs_path')
                granules_json_abs_path = context['task_instance'].xcom_pull('landsat8_product_json_task', key='granules_json_abs_path')
                metadata_xml_abs_path = context['task_instance'].xcom_pull('landsat8_product_json_task', key='metadata_xml_abs_path')
                list_of_files = [product_json_abs_path, granules_json_abs_path, thumbnail_jpeg_abs_path, product_desc_abs_path, metadata_xml_abs_path]
                log.info(list_of_files)
                product = zipfile.ZipFile(os.path.join(product_directory,"product.zip"), 'w')
                for item in list_of_files:
                         product.write(item,item.rsplit('/', 1)[-1])
                product.close()
                return True

class Landsat8GranuleJsonFileOperator(BaseOperator):
        @apply_defaults
        def __init__(self, location_prop, *args, **kwargs):
                self.location_prop = location_prop
                super(Landsat8GranuleJsonFileOperator, self).__init__(*args, **kwargs)

        def execute(self, context):
                
                return True

class LANDSAT8METADATAPlugin(AirflowPlugin):
        name = "landsat8_metadata_plugin"
        operators = [Landsat8MTLReaderOperator, Landsat8ThumbnailOperator, Landsat8ProductDescriptionOperator, Landsat8ProductZipFileOperator]

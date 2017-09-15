from airflow.operators import BaseOperator, BashOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import XCOM_RETURN_KEY
import logging
import os
import s2reader
log = logging.getLogger(__name__)
from pgmagick import Image, Blob
import 	zipfile, json
import shutil
import xml.etree.ElementTree as ET
from geoserver_plugin import generate_wfs_dict, generate_wcs_dict, generate_wms_dict

''' This class will create a compressed, low resolution, square shaped thumbnail for the
original granule. the current approach is generating a new folder that will contain all the metadata files if it wasn't created. if it was created, then Sentinel2ThumbnailOperator will delete it and create a new empty one and then append the created thumbnail to it. by this way, later operators can append to this directory per product.
'''
class Sentinel2ThumbnailOperator(BaseOperator):

        @apply_defaults
        def __init__(self, 
            thumb_size_x, 
            thumb_size_y,
            input_product=None,
            output_dir=None,
            get_inputs_from=None,
            *args, **kwargs):
                self.thumb_size_x = thumb_size_x
                self.thumb_size_y = thumb_size_y
                self.input_product = input_product
                self.output_dir = output_dir
                self.get_inputs_from = get_inputs_from
                super(Sentinel2ThumbnailOperator, self).__init__(*args, **kwargs)

        def execute(self, context):
            products=list()
            ids=[]

            if self.input_product is not None:
                log.info("Processing single product: " +self.input_product)
                products.append(self.input_product)
            elif self.get_inputs_from is not None:
                log.info("Getting inputs from: " +self.get_inputs_from)
                inputs=context['task_instance'].xcom_pull(task_ids=self.get_inputs_from, key=XCOM_RETURN_KEY)
                for input in inputs:
                    products.append(input)                
            else:
                self.downloaded_products = context['task_instance'].xcom_pull('dhus_download_task', key='downloaded_products')
                if self.downloaded_products is not None and len(self.downloaded_products)!=0:
                    products=self.downloaded_products.keys()
                    log.info(self.downloaded_products)
                    for p in self.downloaded_products:
                        ids.append(self.downloaded_products[p]["id"])
                    print "downloaded products keys :",self.downloaded_products.keys()[0]
            
            if products is None or len(products)==0:
                log.info("Nothing to process.")
                return

            thumbnail_paths=list()
            for product in products:
                log.info("Processing {}".format(product))
                with s2reader.open(product) as safe_product:
                    for granule in safe_product.granules:
                        try:
                            zipf = zipfile.ZipFile(product, 'r')
                            imgdata = zipf.read(granule.pvi_path,'r')
                            img = Blob(imgdata)
                            img = Image(img)
                            img.scale(self.thumb_size_x+'x'+self.thumb_size_y)
                            img.quality(80)
                            thumbnail_name = product.strip(".zip")+"/thumbnail.jpg"
                            if os.path.isdir(product.strip(".zip")):
                                product_rmdir_cmd = "rm -r {} ".format(product.strip(".zip"))
                                product_rmdir_BO = BashOperator(task_id="product_rmdir_{}".format(product.split("/")[-1].strip(".zip")), bash_command = product_rmdir_cmd)
                                product_rmdir_BO.execute(context)
                            product_mkdir_cmd = "mkdir {} ".format(product.strip(".zip"))
                            product_mkdir_BO = BashOperator(task_id="product_mkdir_{}".format(product.split("/")[-1].strip(".zip")), bash_command = product_mkdir_cmd)
                            product_mkdir_BO.execute(context)
                            if self.output_dir is not None:
                                thumbnail_name=os.path.join(self.output_dir,"thumbnail.jpeg")
                                log.info("Writing thumbnail to {}".format(thumbnail_name))
                                img.write(thumbnail_name)
                            else:
                                img.write(str(thumbnail_name))
                            thumbnail_paths.append(thumbnail_name)
                            # XCOM expects a single file so we push it here:
                            context['task_instance'].xcom_push(key='thumbnail_jpeg_abs_path', value=str(thumbnail_name))
                            context['task_instance'].xcom_push(key='ids', value=ids)
                            break
                        except BaseException as e:
                            log.error("Unable to extract thumbnail from {}: {}".format(product, e))
            return thumbnail_paths
            
'''
This class is creating the product.zip contents and passing the absolute path per every file so that the Sentinel2ProductZipOperator can generate the product.zip file.
Also, this class is creating the .wld and .prj files which are required by Geoserver in order to be publish the granules successfully. 
'''
class Sentinel2MetadataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, 
        bands_res,
        bands_dict,
        remote_dir,
        GS_WORKSPACE,
        GS_FEATURETYPE,
        GS_LAYER,
        GS_WMS_WIDTH,
        GS_WMS_HEIGHT,
        GS_WMS_FORMAT,
        GS_WCS_SCALE_I,
        GS_WCS_SCALE_J,
        GS_WCS_FORMAT,
        coverage_id,
        get_inputs_from=None,
        *args, **kwargs):
            self.bands_res = bands_res
            self.remote_dir = remote_dir
            self.bands_dict = bands_dict
            self.GS_WORKSPACE = GS_WORKSPACE
            self.GS_LAYER = GS_LAYER
            self.GS_FEATURETYPE = GS_FEATURETYPE
            self.GS_WMS_WIDTH = GS_WMS_WIDTH
            self.GS_WMS_HEIGHT = GS_WMS_HEIGHT
            self.GS_WMS_FORMAT = GS_WMS_FORMAT
            self.GS_WCS_SCALE_I = GS_WCS_SCALE_I
            self.GS_WCS_SCALE_J = GS_WCS_SCALE_J
            self.GS_WCS_FORMAT = GS_WCS_FORMAT            
            self.coverage_id = coverage_id
            self.get_inputs_from = get_inputs_from
            super(Sentinel2MetadataOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        if self.get_inputs_from != None:
            log.info("Getting inputs from: " +self.get_inputs_from)
            self.downloaded_products = context['task_instance'].xcom_pull(task_ids=self.get_inputs_from, key=XCOM_RETURN_KEY)
        else:
            log.info("Getting inputs from: dhus_download_task" )
            self.downloaded_products = context['task_instance'].xcom_pull('dhus_download_task', key='downloaded_products')

        services= [{"wms":("GetCapabilities","GetMap")},{"wfs":("GetCapabilities","GetFeature")},{"wcs":("GetCapabilities","GetCoverage")}]
        for product in self.downloaded_products.keys():
            log.info("Processing: {}".format(product))
            with s2reader.open(product) as s2_product:
                coords = []
                links=[]
                product_footprint = [[[m.replace(" ", ",")] for m in str(s2_product.footprint).replace(", ", ",").partition('((')[-1].rpartition('))')[0].split(",")]]
                for item in product_footprint[0]:
                    [x_coordinate, y_coordinate] = item[0].split(",")
                    coords.append([float(x_coordinate), float(y_coordinate)])
                final_metadata_dict = {"type": "Feature", "geometry":
                {"type": "Polygon", "coordinates":
                [coords]},
                "properties": {"eop:identifier": s2_product.manifest_safe_path.rsplit('.SAFE', 1)[0],
                "timeStart": s2_product.product_start_time,
                "timeEnd": s2_product.product_stop_time,
                "originalPackageLocation": None, "thumbnailURL": None,
                "quicklookURL": None, "eop:parentIdentifier": "SENTINEL2",
                "eop:productionStatus": None, "eop:acquisitionType": None,
                "eop:orbitNumber": s2_product.sensing_orbit_number, "eop:orbitDirection": s2_product.sensing_orbit_direction,
                "eop:track": None, "eop:frame": None, "eop:swathIdentifier": None,
                "opt:cloudCover": None,
                "opt:snowCover": None, "eop:productQualityStatus": None,
                "eop:productQualityDegradationStatus": None,
                "eop:processorName": None,
                "eop:processingCenter": None, "eop:creationDate": None,
                "eop:modificationDate": None,
                "eop:processingDate": None, "eop:sensorMode": None,
                "eop:archivingCenter": None, "eop:processingMode": None,
                "eop:availabilityTime": None,
                "eop:acquisitionStation": None,
                "eop:acquisitionSubtype": None,
                "eop:startTimeFromAscendingNode": None,
                "eop:completionTimeFromAscendingNode": None,
                "eop:illuminationAzimuthAngle": None,
                "eop:illuminationZenithAngle": None,
                "eop:illuminationElevationAngle": None, "eop:resolution": None}}
                for i in self.bands_res.values():
                    features_list = []
                    granule_counter = 1
                    for granule in s2_product.granules:
                        granule_coords = []
                        granule_coordinates = [[[m.replace(" ", ",")] for m in str(granule.footprint).replace(", ", ",").partition('((')[-1].rpartition('))')[0].split(",")]]

                        for item in granule_coordinates[0]:
                            [granule_x_coordinate, granule_y_coordinate] = item[0].split(",")
                            granule_coords.append([float(granule_x_coordinate), float(granule_y_coordinate)])
                        zipped_product = zipfile.ZipFile(product)
                        for file_name in zipped_product.namelist():
                            if file_name.endswith('.jp2') and not file_name.endswith('PVI.jp2'):
                                 features_list.append({"type": "Feature", "geometry": { "type": "Polygon", "coordinates": [granule_coords]},\
                        "properties": {\
                        "location":os.path.join(self.remote_dir, granule.granule_path.rsplit("/")[-1], "IMG_DATA", file_name.rsplit("/")[-1]), "band": self.bands_dict[file_name.rsplit("/")[-1].rsplit(".")[0][-3:]]},\
                        "id": "GRANULE.{}".format(granule_counter)})
                                 granule_counter+=1
            final_granules_dict = {"type": "FeatureCollection", "features": features_list}

            # Note here that the SRID is a property of the granule not the product
            final_metadata_dict["properties"]["crs"] = granule.srid
            with open(product.strip(".zip")+'/product.json', 'w') as product_outfile:
                json.dump(final_metadata_dict, product_outfile,indent=4)                
            with open(product.strip(".zip")+'/granules.json', 'w') as granules_outfile:
                json.dump(final_granules_dict, granules_outfile, indent=4)
            for service in services:
                    service_name, service_calls = service.items()[0]
                    links.append({"offering": "http://www.opengis.net/spec/owc-atom/1.0/req/{}".format(service_name),
                                  "method": "GET",
                                  "code": "GetCapabilities",
                                  "type": "application/xml",
                                  "href": "${BASE_URL}"+"/{}/{}/ows?service={}&request=GetCapabilities&CQL_FILTER=eoParentIdentifier='{}'".format(self.GS_WORKSPACE, self.GS_LAYER, service_name,s2_product.manifest_safe_path.rsplit('.SAFE', 1)[0])})
            #Here we generate the dictionaries of GetMap, GetFeature and GetCoverage operations from util dir
            links.append(generate_wfs_dict(s2_product, self.GS_WORKSPACE, self.GS_FEATURETYPE))
            links.append(generate_wcs_dict(granule_coordinates, self.GS_WORKSPACE, s2_product, self.coverage_id, self.GS_WCS_FORMAT, self.GS_WCS_SCALE_I, self.GS_WCS_SCALE_J))
            links.append(generate_wms_dict(self.GS_WORKSPACE, self.GS_LAYER, granule_coordinates, self.GS_WMS_WIDTH, self.GS_WMS_HEIGHT, self.GS_WMS_FORMAT, s2_product))
            final_owslinks_dict = {"links":links}
            with open(product.strip(".zip")+'/owsLinks.json', 'w') as owslinks_outfile:
                  json.dump(final_owslinks_dict, owslinks_outfile, indent=4)

        self.custom_archived = []
        for archive_line in self.downloaded_products.keys():
            jp2_files_paths = []
            archive_path = archive_line
            archived_product = zipfile.ZipFile(archive_line,'r')
            for file_name in archived_product.namelist():
                if file_name.endswith('.jp2') and not file_name.endswith('PVI.jp2'):
                    archived_product.extract(file_name, archive_path.strip(".zip"))
                    jp2_files_paths.append(os.path.join(archive_path.strip(".zip"),file_name))
                    parent_dir = os.path.dirname(jp2_files_paths[0])
                if file_name.endswith('MTD_TL.xml'):
                    archived_product.extract(file_name, archive_path.strip(".zip"))
                    mtd_tl_xml = os.path.join(archive_path.strip(".zip"),file_name)
            tree = ET.parse(mtd_tl_xml)
            root = tree.getroot()
            geometric_info = root.find(root.tag.split('}', 1)[0]+"}Geometric_Info")
            tile_geocoding = geometric_info.find("Tile_Geocoding")
            wld_files = []
            prj_files = []
            for jp2_file in jp2_files_paths:
                wld_name = os.path.splitext(jp2_file)[0]
                gdalinfo_cmd = "gdalinfo {} > {}".format(jp2_file, wld_name+".prj")
                gdalinfo_BO = BashOperator(task_id="bash_operator_gdalinfo_{}".format(wld_name[-3:]), bash_command = gdalinfo_cmd)
                gdalinfo_BO.execute(context)
                sed_cmd = "sed -i -e '1,4d;29,$d' {}".format(wld_name+".prj")
                sed_BO = BashOperator(task_id="bash_operator_sed_{}".format(wld_name[-3:]), bash_command = sed_cmd)
                sed_BO.execute(context)
                prj_files.append(wld_name+".prj")
                wld_file = open(wld_name+".wld","w")
                wld_files.append(wld_name+".wld")
                for key,value in  self.bands_res.items():
                    if wld_name[-3:] in value:
                        element = key
                geo_position = tile_geocoding.find('.//Geoposition[@resolution="{}"]'.format(element))
                wld_file.write(geo_position.find("XDIM").text + "\n" + "0" + "\n" + "0" +"\n")
                wld_file.write(geo_position.find("YDIM").text + "\n")
                wld_file.write(geo_position.find("ULX").text + "\n")
                wld_file.write(geo_position.find("ULY").text + "\n")
            parent_dir = os.path.dirname(jp2_files_paths[0])
            self.custom_archived.append(os.path.dirname(parent_dir))
            log.info(os.path.dirname(parent_dir))
        log.info(self.custom_archived)
        context['task_instance'].xcom_push(key='downloaded_products', value=self.downloaded_products)
        context['task_instance'].xcom_push(key='downloaded_products_with_wldprj', value=' '.join(self.custom_archived))
        return self.custom_archived


'''
This class is receiving the meta data files paths from the Sentinel2MetadataOperator then creates the product.zip
Later, this class will pass the path of the created product.zip to the next task to publish on Geoserver.
'''
class Sentinel2ProductZipOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        target_dir,
        generated_files,
        placeholders,
        get_inputs_from=None,
        *args, **kwargs):
            self.target_dir = target_dir
            self.generated_files = generated_files
            self.placeholders = placeholders
            self.get_inputs_from = get_inputs_from
            super(Sentinel2ProductZipOperator, self).__init__(*args, **kwargs)            

    def execute(self, context):
        if self.get_inputs_from != None:
            log.info("Getting inputs from: " +self.get_inputs_from)
            self.downloaded_products = context['task_instance'].xcom_pull(task_ids=self.get_inputs_from, key=XCOM_RETURN_KEY)
        else:
            log.info("Getting inputs from: dhus_metadata_task" )
            self.downloaded_products = context['task_instance'].xcom_pull('dhus_metadata_task', key='downloaded_products')

        # stop processing if there are no products
        if self.downloaded_products is None:
            log.info("Nothing to process.")
            return

        product_zip_paths=list()
        for zipf in self.downloaded_products.keys():
            log.info("Product: {}".format(zipf))
            for ph in self.placeholders:
                shutil.copyfile(ph, os.path.join(zipf.strip(".zip"), ph.split("/")[-1]))
            product_zip_path = os.path.join(os.path.join(zipf.strip(".zip"), "product.zip"))
            product_zip = zipfile.ZipFile(product_zip_path , 'w')
            for item in os.listdir(zipf.strip(".zip")):
                if os.path.isfile(os.path.join(zipf.strip(".zip"),item)) and not item.endswith(".zip"):
                     log.info("inside IF")
                     log.info(item)
                     product_zip.write(os.path.join(zipf.strip(".zip"),item), item)
            product_zip.close()
            product_zip_paths.append(product_zip_path)
            context['task_instance'].xcom_push(key='product_zip_path', value=product_zip_path)
        return product_zip_paths

class SENTINEL2Plugin(AirflowPlugin):
    name = "sentinel2_plugin"
    operators = [Sentinel2ThumbnailOperator,Sentinel2MetadataOperator,Sentinel2ProductZipOperator]

from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import logging
import os
import s2reader
log = logging.getLogger(__name__)
from pgmagick import Image, Blob
import zipfile, json
import shutil


''' This class will create a compressed, low resolution, square shaped thumbnail for the
original granule. the current approach is saving the created thumbnail inside the zip file
'''
class Sentinel2ThumbnailOperator(BaseOperator):

        @apply_defaults
        def __init__(self, thumb_size_x, thumb_size_y, *args, **kwargs):
                self.thumb_size_x = thumb_size_x
                self.thumb_size_y = thumb_size_y
                super(Sentinel2ThumbnailOperator, self).__init__(*args, **kwargs)

        def execute(self, context):
            self.downloaded_products = context['task_instance'].xcom_pull('dhus_download_task', key='downloaded_products')
            log.info(self.downloaded_products)
            ids=[]
            for p in self.downloaded_products:
                ids.append(self.downloaded_products[p]["id"])
            print "downloaded products keys :",self.downloaded_products.keys()[0]
            for product in self.downloaded_products.keys():
                with s2reader.open(product) as safe_product:
                  print safe_product.generation_time
                  for granule in safe_product.granules:
                     try:
                         zipf = zipfile.ZipFile(product, 'a')
                         imgdata = zipf.read(granule.pvi_path,'r')
                         img = Blob(imgdata)
                         img = Image(img)
                         img.scale(self.thumb_size_x+'x'+self.thumb_size_y)
                         img.quality(80)
                         thumbnail_path = product.split(".")[0]+".jpg"
                         img.write(str(thumbnail_path))
                         print str(thumbnail_path)
                         print os.path.join(str(thumbnail_path).rsplit('/',1)[-1])
                         #print str(thumbnail_path).split(".")[0]
                         zipf.write(str(thumbnail_path),"product/thumbnail.jpeg")
                     except:
                         return False
            context['task_instance'].xcom_push(key='thumbnail_jpeg_abs_path', value=str(thumbnail_path))
            context['task_instance'].xcom_push(key='ids', value=ids)
            #return os.path.join(self.downloaded_products,"thumbnail.jpeg")


class Sentinel2MetadataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(Sentinel2MetadataOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.downloaded_products = context['task_instance'].xcom_pull('dhus_download_task', key='downloaded_products')
        for product in self.downloaded_products.keys():
            with s2reader.open(product) as s2_product:
                coords = []
                x = [[[m.replace(" ", ",")] for m in str(s2_product.footprint).replace(", ", ",").partition('((')[-1].rpartition('))')[0].split(",")]]
                for item in x[0]:
                    [x, y] = item[0].split(",")
                    coords.append([float(x), float(y)])
                final_metadata_dict = {"type": "Feature", "geometry":
                {"type": "Polygon", "coordinates":
                [coords]},
                "properties": {"eop:identifier": s2_product.manifest_safe_path.rsplit('.SAFE', 1)[0],
                "timeStart": s2_product.product_start_time,
                "timeEnd": s2_product.product_stop_time,
                "originalPackageLocation": None, "thumbnailURL": None,
                "quicklookURL": None, "eop:parentIdentifier": None,
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
                features_list = []
                granule_counter = 1
                for i in range(1,13):
                    for granule in s2_product.granules:
                        coords = []
                        x = [[[m.replace(" ", ",")] for m in str(granule.footprint).replace(", ", ",").partition('((')[-1].rpartition('))')[0].split(",")]]
                        for item in x[0]:
                            [x, y] = item[0].split(",")
                            coords.append([float(x), float(y)])
                        features_list.append({"type": "Feature", "geometry": { "type": "Polygon", "coordinates": [coords]},
                        "properties": {
                        "location":granule.band_path(granule_counter)
                        },
                        "id": "GRANULE.{}".format(granule_counter)
                        })
                        granule_counter+=1
            final_granules_dict = {"type": "FeatureCollection", "features": features_list}
            with open('product.json', 'w') as product_outfile:
                json.dump(final_metadata_dict, product_outfile)
            product_zipf = zipfile.ZipFile(product, 'a')
            product_zipf.write("product.json","product/product.json")
            with open('granules.json', 'w') as granules_outfile:
                json.dump(final_granules_dict, granules_outfile)
            product_zipf.write("granules.json","product/granules.json")
        context['task_instance'].xcom_push(key='downloaded_products', value=self.downloaded_products)


class Sentinel2ProductZipOperator(BaseOperator):

    @apply_defaults
    def __init__(self, target_dir, generated_files, placeholders, *args, **kwargs):
        self.target_dir = target_dir
        self.generated_files = generated_files
        self.placeholders = placeholders
        super(Sentinel2ProductZipOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.downloaded_products = context['task_instance'].xcom_pull('dhus_metadata_task', key='downloaded_products')

        for zipf in self.downloaded_products.keys():
            with zipfile.ZipFile(zipf) as zf:
                dirname = os.path.join(self.target_dir, os.path.splitext(os.path.basename(zipf))[0])
                for item_file in self.generated_files:
                    zf.extract(item_file, path=dirname)
            for ph in self.placeholders:
                shutil.copyfile(ph, os.path.join(dirname, os.path.join("product",ph.split("/")[-1])))
            product_zip = zipfile.ZipFile(os.path.join(zipf.strip(".zip"), "product.zip"), 'a')
            for item in os.listdir(os.path.join(zipf.strip(".zip"),"product")):
                print os.path.join(zipf.strip(".zip"),"product",item)
                product_zip.write(os.path.join(zipf.strip(".zip"),"product",item), item)
            product_zip.close()

class SENTINEL2Plugin(AirflowPlugin):
    name = "sentinel2_plugin"
    operators = [Sentinel2ThumbnailOperator,Sentinel2MetadataOperator,Sentinel2ProductZipOperator]

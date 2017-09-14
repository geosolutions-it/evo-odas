import logging
import pprint
import os

from datetime import datetime, timedelta
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import XCOM_RETURN_KEY

from sentinelsat.sentinel import SentinelAPI, read_geojson, geojson_to_wkt

log = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)

class DHUSSearchOperator(BaseOperator):

    @apply_defaults
    def __init__(self, 
            dhus_url,
            dhus_user,
            dhus_pass,
            geojson_bbox, 
            startdate, 
            enddate,
            filter_max=10,
            order_by='+ingestiondate,-cloudcoverpercentage',            
            keywords=None,
            *args, **kwargs):
        self.dhus_url = dhus_url
        self.dhus_user = dhus_user
        self.dhus_pass = dhus_pass
        self.geojson_bbox = geojson_bbox
        self.startdate = startdate
        self.enddate = enddate
        self.filter_max = filter_max
        self.order_by = order_by        
        self.keywords = keywords

        print("Init DHUS Search.. ")
        
        super(DHUSSearchOperator, self).__init__(*args, **kwargs)
        
    def execute(self, context):
        log.info(context)
        log.info("#################")
        log.info("## DHUS Search ##")
        log.info('API URL: %s', self.dhus_url)
        log.info('API User: %s', self.dhus_user)
        #log.info('API Password: %s', self.dhus_pass)
        log.info('Start Date: %s', self.startdate)
        log.info('End Date: %s', self.enddate)
        log.info('Filter Max: %s', self.filter_max)
        log.info('Order By: %s', self.order_by)        
        log.info('GeoJSON: %s', self.geojson_bbox)
        log.info('Keywords: %s', self.keywords)

        log.info('Now is: {}'.format( datetime.now() ))
        log.info('6 hours ago was: {}'.format( datetime.now() - timedelta(hours=6)) )

        print("Execute DHUS Search.. ")

        # search products 
        api = SentinelAPI(self.dhus_user, self.dhus_pass, self.dhus_url)
        try:
            footprint = geojson_to_wkt(read_geojson(self.geojson_bbox))
        except:
            log.error('Cannot open GeoJSON file: {}'.format(self.geojson_bbox))
            return False

        products = api.query(
            area=footprint,
            date=(self.startdate, self.enddate),
            order_by=self.order_by,
            limit=self.filter_max,
            **self.keywords
        )

        log.info("Retrieving {} products:".format(len(products)))
        products_summary="\n"
        for key, product in products.items():
            products_summary+='ID: {}, {}\n'.format(key,product['summary'])
        log.info(products_summary)

        context['task_instance'].xcom_push(key='searched_products', value=products)
        return products
    
class DHUSDownloadOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
            dhus_url,
            dhus_user,
            dhus_pass,
            download_dir,
            download_timeout=timedelta(hours=1),
            download_max=10,
            get_inputs_from=None,
            product_ids=None,
            *args, **kwargs):
        self.dhus_url = dhus_url
        self.dhus_user = dhus_user
        self.dhus_pass = dhus_pass
        self.download_max = int(download_max)
        self.download_dir = download_dir
        self.product_ids = product_ids
        self.get_inputs_from = get_inputs_from
        
        print("Init DHUS Download.. ")
        
        super(DHUSDownloadOperator, self).__init__(execution_timeout=download_timeout,*args, **kwargs)

    def execute(self, context):
        log.info("###################")
        log.info("## DHUS Download ##")
        log.info('API URL: %s', self.dhus_url)
        log.info('API User: %s', self.dhus_user)
        #log.info('API Password: %s', self.dhus_pass)
        log.info('Max Downloads: %s', self.download_max)
        log.info('Download Directory: %s', self.download_dir)
        log.info('Input from: %s', self.get_inputs_from)
        log.info('Product IDs: %s', self.product_ids)

        log.info("Execute DHUS Download.. ")
        
        # create the download directory if it does not exists
        if not os.path.exists(self.download_dir):
            log.info("Creating directory for download: {}".format(self.download_dir))
            os.makedirs(self.download_dir)

        # generate a dict of products from 
        # 1) the list of product_ids
        # 2) in case 1) is None then check the XCOM key in 'get_inputs_from'
        self.products = dict()
        if self.product_ids != None and len(self.product_ids) != 0:
            for product_id in self.product_ids:
                self.products[product_id]=""
            print("Download request for {} products via IDs:\n{}".format(len(self.product_ids),self.products))
        elif self.get_inputs_from != None:
            self.products = context['task_instance'].xcom_pull(task_ids=self.get_inputs_from, key=XCOM_RETURN_KEY)
            print("Downloading request for {} products via XCOM:\n{}".format(len(self.products), self.products))
        else:
            # exit gracefully if no products are found
            log.info('no products to process')
            return None
    
        # log warning in case the amount of products exceed the limit
        if len(self.products) > self.download_max:
            log.warn("Found products ({}) exceeds download limit ({})".format(len(self.products), self.download_max))
    
        # download all files via it's ID
        log.info('Starting downloading..')
        product_downloaded = {}
        api = SentinelAPI(self.dhus_user, self.dhus_pass, self.dhus_url)
        for product_id in self.products.keys():

            # If download limit reached, stopp and break out
            # Else if the file already exists, then try next from search
            product_filename=os.path.join(self.download_dir,self.products[product_id].get("title")+".zip")
            if len(product_downloaded) >= self.download_max:
                log.info("Limit exceeded, stopping download..")
                break;
            elif os.path.exists(product_filename):
                log.info("Product already downloaded. Continuing..")
                continue

            log.info('Download Product ID {}'.format(product_id))
            downloaded = api.download(product_id, directory_path=self.download_dir);
            path = downloaded['path']
            # TODO check if file in 'path' is binary.
            # It might is an XML file containing an error such as 
            # "Maximum number of 2 concurrent flows achieved by the user "xyz""
            # Check MD5
            # If file already downloaded move on to next one?
            product_downloaded[path] = downloaded;
        
        # print summary and push products to XCOM
        log.debug("Downloaded {} products:\n{}".format(len(product_downloaded),pp.pprint(product_downloaded)))
        context['task_instance'].xcom_push(key='downloaded_products', value=product_downloaded)
        context['task_instance'].xcom_push(key='downloaded_products_paths', value=' '.join(product_downloaded.keys()))
        return product_downloaded

class DHUSPlugin(AirflowPlugin):
    name = "dhus_plugin"
    operators = [DHUSSearchOperator, DHUSDownloadOperator]

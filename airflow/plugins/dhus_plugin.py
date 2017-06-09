import logging
import pprint

from datetime import timedelta
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

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
            platformname=None,
            identifier=None,
            *args, **kwargs):
        self.dhus_url = dhus_url
        self.dhus_user = dhus_user
        self.dhus_pass = dhus_pass
        self.geojson_bbox = geojson_bbox
        self.startdate = str(startdate)
        self.enddate = str(enddate)
        self.platformname = platformname
        self.identifier = identifier

        print("Init DHUS Search.. ")
        
        super(DHUSSearchOperator, self).__init__(*args, **kwargs)
        
    def execute(self, context):
        log.info(context)
        log.info("#################")
        log.info("## DHUS Search ##")
        log.info('API URL: %s', self.dhus_url)
        log.info('API User: %s', self.dhus_user)
        log.info('API Password: %s', self.dhus_pass)
        log.info('Start Date: %s', self.startdate)
        log.info('End Date: %s', self.enddate)
        log.info('GeoJSON: %s', self.geojson_bbox)
        log.info('Platform: %s', self.platformname)
        log.info('Identifier: %s', self.identifier)
        
        print("Execute DHUS Search.. ")

        # search products
        api = SentinelAPI(self.dhus_user, self.dhus_pass, self.dhus_url)
        try:
            footprint = geojson_to_wkt(read_geojson(self.geojson_bbox))
        except:
            log.error('Cannot open GeoJSON file: {}'.format(self.geojson_bbox))
            return False

        products = api.query(
            footprint,
            initial_date=self.startdate,
            end_date=self.enddate,
            platformname=self.platformname,
        )
        
        #product_summary=""
        #for key, product in products.items():
            #product_summary+='{}|{}|{}\n'.format(product['key'],key,product['summary'])
            #log.info('Product: {}\n{} | {}'.format(product['id'],key,product['summary']))
            #log.debug("{}".format( pp.pprint(product)));        
        log.info("Found {} products:\n{}".format(len(products),pprint.pprint(products)))
        log.debug('Pushing to XCom: {}'.format(products))
        context['task_instance'].xcom_push(key='searched_products', value=products)
        return True
    
class DHUSDownloadOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
            dhus_url,
            dhus_user,
            dhus_pass,
            download_dir,
            download_timeout=timedelta(hours=1),
            download_max=100,
            product_ids=None,
            *args, **kwargs):
        self.dhus_url = dhus_url
        self.dhus_user = dhus_user
        self.dhus_pass = dhus_pass
        self.download_max = int(download_max)
        self.download_dir = download_dir
        self.product_ids = product_ids
        
        print("Init DHUS Download.. ")        
        
        super(DHUSDownloadOperator, self).__init__(execution_timeout=download_timeout,*args, **kwargs)

    def execute(self, context):
        log.info("###################")
        log.info("## DHUS Download ##")
        log.info('API URL: %s', self.dhus_url)
        log.info('API User: %s', self.dhus_user)
        log.info('API Password: %s', self.dhus_pass)
        log.info('Max Downloads: %s', self.download_max)
        log.info('Download Directory: %s', self.download_dir)

        log.info("Execute DHUS Download.. ")
        
        if self.product_ids == None:
            self.product_ids = []
            
        # retrieving products from previous search step
        self.products = context['task_instance'].xcom_pull('dhus_search_task', key='searched_products')
	print('type: {}'.format(self.products))
        log.info("Retrieved {} products:\n{}".format(len(self.products), pprint.pprint(self.products)))

        if not self.products or len(self.products) == 0:
            log.info('no products to process')
            return True
    
        if len(self.product_ids) > self.download_max:
            log.warn("Found products ({}) exceeds download limit ({})".format(len(self.product_ids), self.download_max))
    
        log.info('Downloading up to {} products..'.format(self.download_max))
        product_downloaded = {}
        api = SentinelAPI(self.dhus_user, self.dhus_pass, self.dhus_url)
        for product_id in self.products.keys():
            if len(product_downloaded) >= self.download_max:
                break;
            log.info('Download Product ID {}'.format(product_id))
            downloaded = api.download(product_id, directory_path=self.download_dir);
            path = downloaded['path']
            # TODO check if file in 'path' is binary.
            # It might is an XML file containing an error such as 
            # "Maximum number of 2 concurrent flows achieved by the user "xyz""
            # Check MD5
            # If file already downloaded move on to next one?
            product_downloaded[path] = downloaded;
        
        log.debug("Downloaded {} products:\n{}".format(len(product_downloaded),pp.pprint(product_downloaded)))
        context['task_instance'].xcom_push(key='downloaded_products', value=product_downloaded)
        return True

class DHUSPlugin(AirflowPlugin):
    name = "dhus_plugin"
    operators = [DHUSSearchOperator, DHUSDownloadOperator]

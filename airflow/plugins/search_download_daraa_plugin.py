from airflow import DAG
import logging
import psycopg2
import urllib
from datetime import datetime, timedelta
from airflow.operators import BaseOperator, BashOperator 
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import os

log = logging.getLogger(__name__)

class Landsat8SearchOperator(BaseOperator):

    @apply_defaults
    def __init__(self, 
            cloud_coverage, 
            path, 
            row,
            pgdbname,
            pghostname,
            pgport,
            pgusername,
            pgpassword,
            processing_level="L1TP",
            *args, **kwargs):
        self.cloud_coverage = cloud_coverage
        #self.acquisition_date = str(acquisition_date)
        self.path = path
        self.row = row
        self.processing_level = processing_level
        self.pgdbname = pgdbname
        self.pghostname = pghostname
        self.pgport = pgport
        self.pgusername = pgusername
        self.pgpassword = pgpassword
        print("Initialization of Daraa Landsat8SearchOperator ...")
        super(Landsat8SearchOperator, self).__init__(*args, **kwargs)
        
    def execute(self, context):
        log.info(context)
        log.info("#####################"		)
        log.info("## LANDSAT8 Search ##")
        log.info('Cloud Coverage <= % : %f', self.cloud_coverage)
        #log.info('Acquisition Date : %s', self.acquisition_date)
        log.info('Path : %d', self.path)
        log.info('Row : %d', self.row)
        log.info('Processing Level: %s', self.processing_level)
        print("Executing Landsat8SearchOperator .. ")

        db = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(self.pgdbname, self.pgusername, self.pghostname, self.pgpassword))
        cursor = db.cursor()
        sql_stmt = 'select productId, entityId, download_url from scene_list where cloudCover < {} and path = {} and row = {} order by acquisitionDate desc limit 1'.format(self.cloud_coverage,self.path,self.row)
        cursor.execute(sql_stmt)
        result_set = cursor.fetchall()
        print result_set
        log.info("Found {} product with {} scene id, available for download through {} ".format(result_set[0][0],result_set[0][1],result_set[0][2]))
        context['task_instance'].xcom_push(key='searched_products', value=result_set[0])
        return True

class Landsat8DownloadOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
            download_dir,
            number_of_bands = None,
            download_timeout=timedelta(hours=1),
            *args, **kwargs):
        self.download_dir = download_dir
        self.number_of_bands = number_of_bands
	log.info("----------------------------------------------------")
        print("Initialization of Landsat8 Download ... ")
	log.info('Download Directory: %s', self.download_dir)
        super(Landsat8DownloadOperator, self).__init__(execution_timeout=download_timeout,*args, **kwargs)

    def execute(self, context):
        log.info("#######################")
        log.info("## Landsat8 Download ##")
        log.info('Download Directory: %s', self.download_dir)
        print("Execute Landsat8 Download ... ")
        scene_url = context['task_instance'].xcom_pull('landsat8_search_daraa_task', key='searched_products')
	log.info("#######################")
	log.info(self.download_dir+scene_url[1])
        if os.path.isdir(self.download_dir+scene_url[1]):
           pass
        else:
           create_dir = BashOperator(task_id="bash_operator_translate_daraa", bash_command="mkdir {}".format(self.download_dir+scene_url[1]))
           create_dir.execute(context)
        counter = 1
	try:
		urllib.urlretrieve(os.path.join(scene_url[2].replace("index.html",scene_url[0]+"_MTL.txt")),os.path.join(self.download_dir+scene_url[1],scene_url[0]+'_MTL.txt'))
		urllib.urlretrieve(os.path.join(scene_url[2].replace("index.html",scene_url[0]+"_thumb_small.jpg")),os.path.join(self.download_dir+scene_url[1],scene_url[0]+'_thumb_small.jpg'))
		while counter <= self.number_of_bands:
			urllib.urlretrieve(scene_url[2].replace("index.html",scene_url[0]+"_B"+str(counter)+".TIF"),os.path.join(self.download_dir+scene_url[1],scene_url[0]+'_B'+str(counter)+'.TIF'))
			counter+=1
	except:
		log.info("EXCEPTION: ### Download not completed successfully, please check all the scenes, mtl and small jpg ###")
        context['task_instance'].xcom_push(key='scene_fullpath', value=self.download_dir+scene_url[1])
        return True


class SearchDownloadDaraaPlugin(AirflowPlugin):
    name = "search_download_daraa_plugin"
    operators = [Landsat8SearchOperator, Landsat8DownloadOperator]

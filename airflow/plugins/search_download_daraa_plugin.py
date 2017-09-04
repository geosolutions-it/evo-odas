from datetime import datetime
from datetime import timedelta
from itertools import chain
import logging
import os
import psycopg2
import urllib

from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class Landsat8SearchOperator(BaseOperator):

    @apply_defaults
    def __init__(self, cloud_coverage, path, row, pgdbname, pghostname,
                 pgport, pgusername, pgpassword, processing_level="L1TP",
                 *args, **kwargs):
        self.cloud_coverage = cloud_coverage
        # self.acquisition_date = str(acquisition_date)
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
        log.info("#####################")
        log.info("## LANDSAT8 Search ##")
        log.info('Cloud Coverage <= % : %f', self.cloud_coverage)
        # log.info('Acquisition Date : %s', self.acquisition_date)
        log.info('Path : %d', self.path)
        log.info('Row : %d', self.row)
        log.info('Processing Level: %s', self.processing_level)
        print("Executing Landsat8SearchOperator .. ")

        db = psycopg2.connect(
            "dbname='{}' user='{}' host='{}' password='{}'".format(
                self.pgdbname, self.pgusername, self.pghostname,
                self.pgpassword
            )
        )
        cursor = db.cursor()
        sql_stmt = (
            'select productId, entityId, download_url '
            'from scene_list '
            'where cloudCover < {} and path = {} and row = {} '
            'order by acquisitionDate desc '
            'limit 1'.format(self.cloud_coverage, self.path, self.row)
        )
        cursor.execute(sql_stmt)
        result_set = cursor.fetchall()
        print(result_set)
        log.info(
            "Found {} product with {} scene id, available for download "
            "through {} ".format(result_set[0][0],
                                 result_set[0][1],
                                 result_set[0][2])
        )
        context['task_instance'].xcom_push(
            key='searched_products', value=result_set[0])
        return True


class Landsat8DownloadOperator(BaseOperator):

    @apply_defaults
    def __init__(self, download_dir, bands=None,
                 download_timeout=timedelta(hours=1),
                 *args, **kwargs):
        super(Landsat8DownloadOperator, self).__init__(
            execution_timeout=download_timeout, *args, **kwargs)
        self.download_dir = download_dir
        self.bands = [int(b) for b in bands] if bands is not None else [1]

    def execute(self, context):
        task_inputs = context["task_instance"].xcom_pull(
            'landsat8_search_daraa_task',
            key='searched_products'
        )
        product_id, entity_id, download_url = task_inputs
        target_dir = self.download_dir + entity_id
        try:
            os.makedirs(target_dir)
        except OSError as exc:
            if exc.errno == 17:  # directory already exists
                pass
        file_endings = chain(
            ("MTL.txt", "thumb_small.jpg"),
            ("B{}.TIF".format(i) for i in self.bands)
        )
        # TODO: Download files in parallel instead of sequentially
        for ending in file_endings:
            url = download_url.replace(
                "index.html",
                "{id}_{ending}".format(id=product_id, ending=ending)
            )
            target_path = os.path.join(
                self.download_dir, entity_id,
                "{}_{}".format(product_id, ending)
            )
            try:
                urllib.urlretrieve(url, target_path)
            except Exception:
                log.exception(
                    msg="EXCEPTION: ### Error downloading {}".format(ending))
        context['task_instance'].xcom_push(key='scene_fullpath',
                                           value=target_dir)
        return True


class SearchDownloadDaraaPlugin(AirflowPlugin):
    name = "search_download_daraa_plugin"
    operators = [
        Landsat8SearchOperator,
        Landsat8DownloadOperator
    ]

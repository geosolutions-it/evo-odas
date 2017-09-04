from datetime import datetime
from datetime import timedelta
import logging

from airflow.models import DAG
from airflow.operators import BaseOperator
from airflow.operators import BashOperator
from airflow.operators import GDALAddoOperator
from airflow.operators import GDALTranslateOperator
from airflow.operators import Landsat8DownloadOperator
from airflow.operators import Landsat8MTLReaderOperator
from airflow.operators import Landsat8ProductDescriptionOperator
from airflow.operators import Landsat8ProductZipFileOperator
from airflow.operators import Landsat8SearchOperator
from airflow.operators import Landsat8ThumbnailOperator

from landsat8.secrets import postgresql_credentials


daraa_dag = DAG(
    'Search_daraa_Landsat8',
    description='DAG for searching Daraa AOI in Landsat8 data from '
                'scenes_list',
    default_args={
        'start_date': datetime(2017, 1, 1),
        'owner': 'airflow',
        'depends_on_past': False,
        'provide_context': True,
        'email': ['xyz@xyz.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'max_threads': 1,
    },
    dagrun_timeout=timedelta(hours=1),
    schedule_interval=timedelta(days=1),
    catchup=False
)

search_daraa_task = Landsat8SearchOperator(
    task_id='landsat8_search_daraa_task',
    cloud_coverage=90.9,
    path=174,
    row=37,
    pgdbname=postgresql_credentials['dbname'],
    pghostname=postgresql_credentials['hostname'],
    pgport=postgresql_credentials['port'],
    pgusername=postgresql_credentials['username'],
    pgpassword=postgresql_credentials['password'],
    dag=daraa_dag
)

download_daraa_task = Landsat8DownloadOperator(
    task_id='landsat8_download_daraa_task',
    download_dir="/var/data/download",
    bands=[1],
    dag=daraa_dag
)

translate_daraa_task = GDALTranslateOperator(
    task_id='landsat8_translate_daraa_task',
    working_dir="/var/data/download",
    blockx_size="512",
    blocky_size="512",
    tiled="YES",
    b="1",
    ot="UInt16",
    of="GTiff",
    dag=daraa_dag
)

addo_daraa_task = GDALAddoOperator(
    task_id='landsat8_addo_daraa_task',
    xk_pull_dag_id='Search_daraa_Landsat8',
    xk_pull_task_id='landsat8_translate_daraa_task',
    xk_pull_key_srcfile='translated_scenes_dir',
    resampling_method="average",
    max_overview_level=128,
    compress_overview="PACKBITS",
    photometric_overview="MINISBLACK",
    interleave_overview="",
    dag=daraa_dag
)

product_json_task = Landsat8MTLReaderOperator(
    task_id='landsat8_product_json_task',
    loc_base_dir='/efs/geoserver_data/coverages/landsat8/daraa',
    metadata_xml_path='./geo-solutions-work/evo-odas/metadata-ingestion/'
                      'templates/metadata.xml',
    dag=daraa_dag
)

product_thumbnail_task = Landsat8ThumbnailOperator(
    task_id='landsat8_product_thumbnail_task',
    thumb_size_x="64",
    thumb_size_y="64",
    dag=daraa_dag
)

product_description_task = Landsat8ProductDescriptionOperator(
    description_template='./geo-solutions-work/evo-odas/metadata-ingestion/'
                         'templates/product_abstract.html',
    task_id='landsat8_product_description_task',
    dag=daraa_dag
)

product_zip_task = Landsat8ProductZipFileOperator(
    task_id='landsat8_product_zip_task',
    dag=daraa_dag
)

search_daraa_task.set_downstream(download_daraa_task)
download_daraa_task.set_downstream(translate_daraa_task)
translate_daraa_task.set_downstream(addo_daraa_task)
addo_daraa_task.set_downstream(product_json_task)
product_json_task.set_downstream(product_thumbnail_task)
product_thumbnail_task.set_downstream(product_description_task)
product_description_task.set_downstream(product_zip_task)

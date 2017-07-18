from airflow.models import DAG
from airflow.operators import BashOperator, Landsat8SearchOperator, Landsat8DownloadOperator
import logging
from datetime import datetime
from datetime import timedelta

daraa_args = {
    ##################################################
    # General configuration
    ##################################################
    'start_date': datetime(2017, 1, 1),
    'owner': 'airflow',
    'depends_on_past': False,
    'provide_context': True,
    'email': ['xyz@xyz.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'max_threads': 1,
    #################################################
    # Search landsat args
    #################################################
    #'acquisition_date': '2017-04-11 05:36:29.349932',
    'cloud_coverage': 90.9,
    'path': 174,
    'row' : 37,
    #################################################
    # Download landsat args
    #################################################
    'download_dir': '/home/moataz/airflow/data/downloads/',
    'number_of_bands' : 2
    #################################################
    # Translate landsat args
    #################################################
    #'working_dir' : '/home/moataz/airflow/data',
    #'blockx_size' : '512',
    #'blocky_size' : '512',
    #'tiled'       : 'YES',
    #'compress'    : 'PACKBITS',
    #'photometric' : '',
    #'b'           : '1',
    #'ot'          : '',
    #'of'          : 'GTiff',
    #'outsize'     : '',
    #'scale'       : '',
    #################################################
    #  Addo landsat args
    #################################################
    #'resampling_method'     : 'average',
    #'max_overview_level'    : '128',
    #'compress_overview'     : 'PACKBITS',
    #'photometric_overview'  : 'MINISBLACK',
    #'interleave_overview'   : ''
}


# DAG definition
daraa_dag = DAG('Search_daraa_Landsat8', description='DAG for searching Daraa AOI in Landsat8 data from scenes_list',
          default_args=daraa_args,
          dagrun_timeout=timedelta(hours=1),
          schedule_interval=timedelta(days=1),
          catchup=False)

# Landsat Search Task Operator
search_daraa_task = Landsat8SearchOperator(task_id= 'landsat8_search_daraa_task', dag = daraa_dag)
# Landsat Download Task Operator
download_daraa_task = Landsat8DownloadOperator(task_id= 'landsat8_download_daraa_task', dag = daraa_dag)
# Landsat GDAL Translate task Operator
#translate_daraa_task = GDALTranslateLandsatOperator(task_id= 'landsat8_translate_daraa_task', dag = daraa_dag)
# Landsat Addo task Operator
#addo_daraa_task = GDALAddoLandsatOperator(task_id= 'landsat8_addo_daraa_task', dag = daraa_dag)

search_daraa_task >> download_daraa_task 

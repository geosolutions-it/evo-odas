import os
import logging
from sentinel1.secrets import dhus_credentials
from airflow import DAG
from airflow.operators import DHUSSearchOperator, DHUSDownloadOperator

from datetime import datetime, timedelta

log = logging.getLogger(__name__)

# Settings
default_args = {
    ##################################################
    # General configuration
    #
    'start_date': datetime.today() - timedelta(days=1),
    'owner': 'airflow',
    'depends_on_past': False,
    'provide_context': True,
    'email': ['airflow@evoodas.dlr.de'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'max_threads': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    #
}

search_task_args = {
    'dhus_url': 'https://scihub.copernicus.eu/dhus',
    'dhus_user': dhus_credentials['username'],
    'dhus_pass': dhus_credentials['password'],
    'geojson_bbox': '/var/data/regions/europe.geojson',
    'startdate': (datetime.today() - timedelta(days=30)).isoformat() + 'Z',
    'enddate': datetime.now().isoformat() + 'Z',
    'platformname': 'Sentinel-1',
    'filename': 'S1?_EW_GRDM_1S*',
}

download_base_dir= '/var/data/download/'
download_dir= os.path.join(download_base_dir, search_task_args['platformname'], 'GRD')
download_task_args = {
    'dhus_url': 'https://scihub.copernicus.eu/dhus',
    'dhus_user': dhus_credentials['username'],
    'dhus_pass': dhus_credentials['password'],
    'download_max': '1',
    'download_dir': '/var/data/download/',
}

# DAG definition
dag = DAG('S1_Download_EW_GRDM_1SDH', description='DAG for searching, filtering and downloading Sentinel data from DHUS server',
          default_args=default_args,
          dagrun_timeout=timedelta(hours=1),
          schedule_interval='0 11 * * *',
          catchup=False)

# DHUS Search Task Operator
search_task = DHUSSearchOperator(task_id='dhus_search_task',
                                 dhus_url=search_task_args['dhus_url'],
                                 dhus_user=search_task_args['dhus_user'],
                                 dhus_pass=search_task_args['dhus_pass'],
                                 geojson_bbox=search_task_args['geojson_bbox'],
                                 startdate=search_task_args['startdate'],
                                 enddate=search_task_args['enddate'],
                                 platformname=search_task_args['platformname'],
                                 filename=search_task_args['filename'],
                                 dag=dag)

# DHUS Download Task Operator
download_task = DHUSDownloadOperator(task_id='dhus_download_task',
                                     dhus_url=download_task_args['dhus_url'],
                                     dhus_user=download_task_args['dhus_user'],
                                     dhus_pass=download_task_args['dhus_pass'],
                                     download_max=download_task_args['download_max'],
                                     download_dir=download_task_args['download_dir'],
                                     dag=dag)

search_task >> download_task

import os
import logging
from sentinel1.config import dhus_config
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
    ##################################################
    # Search and Download plugin configuration
    'dhus_url': dhus_config['dhus_url'],
    'dhus_user': dhus_config['dhus_user'],
    'dhus_pass': dhus_config['dhus_pass'],
    'download_base_dir': '/var/data/download/',
    'download_max': '1',
    'geojson_bbox': '/var/data/regions/europe.geojson',
    'startdate': (datetime.today() - timedelta(days=30)).isoformat() + 'Z',
    'enddate': datetime.now().isoformat() + 'Z',
    'platformname': 'Sentinel-1',
    'filename': 'S1?_EW_GRDM_1S*',
}

download_dir = os.path.join(default_args['download_base_dir'], default_args['platformname'], 'GRD')

# DAG definition
dag = DAG('S1_Download_EW_GRDM_1SDH', description='DAG for searching, filtering and downloading Sentinel data from DHUS server',
          default_args=default_args,
          dagrun_timeout=timedelta(hours=1),
          schedule_interval='0 11 * * *',
          catchup=False)

# DHUS Search Task Operator
search_task = DHUSSearchOperator(task_id='dhus_search_task', dag=dag)

# DHUS Download Task Operator
download_task = DHUSDownloadOperator(task_id='dhus_download_task', dag=dag, download_dir=download_dir)

search_task >> download_task 

import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import DHUSSearchOperator, DHUSDownloadOperator
from sentinel1.secrets import dhus_credentials
from sentinel1.config import ew_grdm_1sdh_config


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

# DAG definition
dag = DAG('S1_Download_EW_GRDM_1SDH', description='DAG for searching, filtering and downloading Sentinel data from DHUS server',
          default_args=default_args,
          dagrun_timeout=timedelta(hours=1),
          schedule_interval='0 11 * * *',
          catchup=False)

# DHUS Search Task Operator
search_task = DHUSSearchOperator(task_id='dhus_search_task',
                                 dhus_url='https://scihub.copernicus.eu/dhus',
                                 dhus_user=dhus_credentials['username'],
                                 dhus_pass=dhus_credentials['password'],
                                 geojson_bbox=ew_grdm_1sdh_config['geojson_bbox'],
                                 startdate=ew_grdm_1sdh_config['startdate'],
                                 enddate=ew_grdm_1sdh_config['enddate'],
                                 platformname=ew_grdm_1sdh_config['platformname'],
                                 filename=ew_grdm_1sdh_config['filename'],
                                 dag=dag)

# DHUS Download Task Operator
download_task = DHUSDownloadOperator(task_id='dhus_download_task',
                                     dhus_url='https://scihub.copernicus.eu/dhus',
                                     dhus_user=dhus_credentials['username'],
                                     dhus_pass=dhus_credentials['password'],
                                     download_max=ew_grdm_1sdh_config['download_max'],
                                     download_dir=ew_grdm_1sdh_config['download_dir'],
                                     dag=dag)

search_task >> download_task

from airflow import DAG
from airflow.operators import DHUSSearchOperator, DHUSDownloadOperator

from datetime import datetime
from datetime import timedelta


# Settings
default_args = {
    ##################################################
    # General configuration
    #
    'start_date': datetime(2017, 1, 1),
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
    # Download DAG configuration
    #
    #dhus_url = 'https://dehub.dlr.de/dhus'
    'dhus_url': 'https://scihub.copernicus.eu/dhus',
    'dhus_user': '*******',
    'dhus_pass': '*******',
    'download_dir': '/var/data/download',
    'download_max': '1',
    #'geojson_bbox': '/var/data/regions/munich.geojson',
    'geojson_bbox': '/var/data/regions/germany.geojson',
    #
    # ------------------------------------------------
    # Sentinel 1 Products
    #
    #'startdate': (datetime.today() - timedelta(5)).isoformat() + 'Z',
    #'enddate': datetime.now().isoformat() + 'Z',
    #'platformname': 'Sentinel-1',
    #'identifier': 'S1?_IW_SLC*',
    #'identifier': 'S1?_IW_GRD*',
    #
    # ------------------------------------------------
    # Sentinel 2
    #
    'startdate': '2017-05-10T10:30:00Z',
    'enddate': '2017-05-10T10:31:00Z',
    'platformname': 'Sentinel-2',
    #'identifier': 'S2?_MSIL1C_*',
    'identifier': 'S2A_MSIL1C_20170510T103031_N0205_R108_T32UPV_20170510T103025'
    #'product_ids': ['dc1486c4-a128-45ca-8a96-5b48da99e9a2']
    #
    # ------------------------------------------------
    # Sentinel 3 Products -> not yet supported
    #
    #'startdate': '2016-05-09T00:00:00Z',
    #'enddate': '2016-05-10T00:00:00Z',
    #'platformname': 'Sentinel-3',
    #
}

# DAG definition
dag = DAG('download_dag', description='DAG for searching, filtering and downloading Sentinel data from DHUS server',
          default_args=default_args,
          dagrun_timeout=timedelta(hours=1),
          schedule_interval=timedelta(minutes=1),
          #schedule_interval='* * * * *', # every minute
          #schedule_interval='* 1 * * *', # each day at 1 am
          catchup=False)

# DHUS Search Task Operator
search_task = DHUSSearchOperator(task_id='dhus_search_task', dag=dag)

# DHUS Download Task Operator
download_task = DHUSDownloadOperator(task_id='dhus_download_task', dag=dag)

search_task >> download_task 
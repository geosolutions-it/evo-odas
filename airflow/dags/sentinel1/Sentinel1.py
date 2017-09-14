import os
import logging
from datetime import datetime, timedelta

from sentinel1.config import ew_grdm_1sdv_config, geoserver_url
from sentinel1.secrets import dhus_credentials, geoserver_credentials
from sentinel1.s1_grd_subdag_factory import gdal_processing_sub_dag

from airflow import DAG
from airflow.operators import BashOperator, DHUSSearchOperator, DHUSDownloadOperator, ZipInspector, MockDownload, SubDagOperator, PythonOperator, RSYNCOperator
from airflow.operators import S1MetadataOperator

log = logging.getLogger(__name__)

INTERVAL = '0 11 * * *'
DATE = datetime(2017, 5, 4)
DAG_NAME = 'Sentinel1'
SUBDAG_NAME = 'sentinel1_gdal'
COLLECTION_NAME='SENTINEL1'
WORKING_DIR='/var/data/working'

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
    'retries': 0,
    'max_threads': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    #
}

# DAG definition
main_dag = DAG(DAG_NAME, description='DAG for searching, filtering and downloading Sentinel data from DHUS server',
          default_args=default_args,
          dagrun_timeout=timedelta(hours=1),
          schedule_interval='0 6 * * *',
          catchup=False)

sentinel1_gdal_task = SubDagOperator(
    subdag = gdal_processing_sub_dag(DAG_NAME, SUBDAG_NAME, DATE, INTERVAL,working_dir="/var/data/working"),
    task_id=SUBDAG_NAME,
    dag=main_dag
)

# DHUS Search Task Operator
search_task = DHUSSearchOperator(task_id='dhus_search_task',
                                 dhus_url='https://scihub.copernicus.eu/dhus',
                                 dhus_user=dhus_credentials['username'],
                                 dhus_pass=dhus_credentials['password'],
                                 geojson_bbox=ew_grdm_1sdv_config['geojson_bbox'],
                                 startdate=ew_grdm_1sdv_config['startdate'],
                                 enddate=ew_grdm_1sdv_config['enddate'],
                                 keywords = ew_grdm_1sdv_config['search_keywords'],
                                 dag=main_dag)

# DHUS Download Task Operator
download_task = DHUSDownloadOperator(task_id='dhus_download_task',
                                     dhus_url='https://scihub.copernicus.eu/dhus',
                                     dhus_user=dhus_credentials['username'],
                                     dhus_pass=dhus_credentials['password'],
                                     download_max=ew_grdm_1sdv_config['download_max'],
                                     download_dir=ew_grdm_1sdv_config['download_dir'],
                                     dag=main_dag)

zip_task = ZipInspector(
    extension_to_search='tiff',
    task_id='zip_inspector',
    dag=main_dag
)

'''
sentinel1_upload_original_package = RSYNCOperator(
    task_id="s1_upload_original_package",
    host = "geoserver.cloudsdi.geo-solutions.it",
    remote_usr = "ec2-user",
    ssh_key_file = "/usr/local/airflow/id_rsa",
    remote_dir = "/efs/geoserver_data/coverages/sentinel/sentinel1/grd/EW_GRDM_1SDV_packages",
    files_to_upload = granules_paths)
'''

sentinel1_upload_granule_1 = RSYNCOperator(
    dag=main_dag,
    task_id="sentinel1_upload_granule_1",
    host = "geoserver.cloudsdi.geo-solutions.it",
    remote_usr = "ec2-user",
    ssh_key_file = "/usr/local/airflow/id_rsa",
    remote_dir = ew_grdm_1sdv_config['granules_upload_dir'],
    xk_pull_dag_id = DAG_NAME + '.sentinel1_gdal',
    xk_pull_task_id = 'gdal_addo_1',
    xk_pull_key = 'dstfile'
    )

sentinel1_upload_granule_2 = RSYNCOperator(
    dag=main_dag,
    task_id="sentinel1_upload_granule_2",
    host = "geoserver.cloudsdi.geo-solutions.it",
    remote_usr = "ec2-user",
    ssh_key_file = "/usr/local/airflow/id_rsa",
    remote_dir = ew_grdm_1sdv_config['granules_upload_dir'],
    xk_pull_dag_id = DAG_NAME + '.sentinel1_gdal',
    xk_pull_task_id='gdal_addo_2',
    xk_pull_key='dstfile'
)

metadata_task = S1MetadataOperator(
    task_id="s1_metadata_extraction",
    dag=main_dag,
    working_dir="/var/data/working",
    product_safe_path=None,
    granules_paths=None,
    granules_upload_dir=ew_grdm_1sdv_config['granules_upload_dir'],
    xk_pull_task_id='dhus_download_task',
    xk_pull_key_safe='downloaded_products'
)

zip_path= os.path.join(WORKING_DIR , 'product.zip')
geoserver_user_pass = geoserver_credentials['username'] + ':' + geoserver_credentials['password']
curl_command='curl -v -u "{}" -XPOST -H "Content-type: application/zip" --data-binary @{}  "{}/rest/oseo/collections/{}/products"'.format(
    geoserver_user_pass, zip_path, geoserver_url, COLLECTION_NAME)
rm_command='rm -f ' + zip_path

publish_product = BashOperator(
        task_id='publish_product',
        bash_command=curl_command + ' && ' + rm_command,
        dag=main_dag
)

search_task >> download_task >> zip_task >> sentinel1_gdal_task >> metadata_task >> sentinel1_upload_granule_1 >> sentinel1_upload_granule_2 >> publish_product

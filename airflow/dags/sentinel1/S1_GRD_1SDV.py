import logging, os, inspect
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import BashOperator, DHUSSearchOperator, DHUSDownloadOperator, ZipInspector, MockDownload, SubDagOperator, PythonOperator, RSYNCOperator
from airflow.operators import S1MetadataOperator
from airflow.models import XCOM_RETURN_KEY

from geoserver_plugin import publish_product
from sentinel1.s1_grd_subdag_factory import gdal_processing_sub_dag
import config as CFG
import config.s1_grd_1sdv as S1GRD1SDV

log = logging.getLogger(__name__)

# Settings
default_args = {
    ##################################################
    # General configuration
    #
    'start_date': datetime.now() - timedelta(hours=1),
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

print("#######################")
print("ID: {}".format(S1GRD1SDV.id))
print("DHUS:  {} @ {}, Region: {}".format(CFG.dhus_username, CFG.dhus_url, S1GRD1SDV.dhus_search_bbox) )
print("GeoServer: {} @ {}".format(CFG.geoserver_username, CFG.geoserver_rest_url) )
print("RSYNC: {} @ {} using {}".format(CFG.rsync_username, CFG.rsync_hostname, CFG.rsync_ssh_key))
print("Date: {} / {}".format(S1GRD1SDV.dhus_search_startdate, S1GRD1SDV.dhus_search_enddate))
print("Search: max={}, order_by={}, keywords={}".format(S1GRD1SDV.dhus_filter_max, S1GRD1SDV.dhus_search_orderby,S1GRD1SDV.dhus_search_keywords))
print("Paths:\n  collection_dir={}\n  download_dir={}\n  process_dir={}\n  upload_dir={}\n  repository_dir={}".format(S1GRD1SDV.collection_dir, S1GRD1SDV.download_dir, S1GRD1SDV.process_dir, S1GRD1SDV.upload_dir, S1GRD1SDV.repository_dir))
print("Collection:\n  workspace={}\n  layer={}".format(S1GRD1SDV.geoserver_workspace, S1GRD1SDV.geoserver_layer))
print("#######################")

# DAG definition
dag = DAG(S1GRD1SDV.id, 
          description='DAG for searching, filtering and downloading Sentinel 1 data from DHUS server',
          schedule_interval='@hourly',
          default_args=default_args
)

# DHUS Search Task Operator
search_task = DHUSSearchOperator(task_id='search_product_task',
                                 dhus_url=CFG.dhus_url,
                                 dhus_user=CFG.dhus_username,
                                 dhus_pass=CFG.dhus_password,
                                 geojson_bbox=S1GRD1SDV.dhus_search_bbox,
                                 startdate=S1GRD1SDV.dhus_search_startdate,
                                 enddate=S1GRD1SDV.dhus_search_enddate,
                                 filter_max=S1GRD1SDV.dhus_filter_max,
                                 order_by=S1GRD1SDV.dhus_search_orderby,
                                 keywords=S1GRD1SDV.dhus_search_keywords,
                                 dag=dag)

# DHUS Download Task Operator
download_task = DHUSDownloadOperator(task_id='download_product_task',
                                     dhus_url=CFG.dhus_url,
                                     dhus_user=CFG.dhus_username,
                                     dhus_pass=CFG.dhus_password,
                                     download_max=S1GRD1SDV.dhus_download_max,
                                     download_dir=S1GRD1SDV.download_dir,
                                     get_inputs_from=search_task.task_id,
                                     download_timeout=timedelta(hours=12),
                                     dag=dag)

# Zip Inspector and Extractor Task
zip_task = ZipInspector(task_id='zip_inspector',
                        extension_to_search='tiff',
                        get_inputs_from=download_task.task_id,
                        dag=dag)

# GDAL Processing SubDAG
sentinel1_gdal_task = SubDagOperator(task_id='sentinel1_gdal',
                                     subdag = gdal_processing_sub_dag(S1GRD1SDV.id, 
                                                                      'sentinel1_gdal', 
                                                                      datetime(2017, 5, 4), 
                                                                      "0 11 * * *",
                                                                      working_dir=S1GRD1SDV.process_dir),
                                     dag=dag)

# Metadata Extraction task
metadata_task = S1MetadataOperator(task_id="extract_metadata_task",
                                   product_safe_path=None,
                                   granules_paths=None,
                                   granules_upload_dir=S1GRD1SDV.repository_dir,
                                   working_dir=S1GRD1SDV.process_dir,
                                   get_inputs_from=download_task.task_id,
                                   dag=dag)

# Rsync Archive Task for Granules
upload_task = RSYNCOperator(task_id="upload_granules_task", 
                            host = CFG.rsync_hostname, 
                            remote_usr = CFG.rsync_username,
                            ssh_key_file = CFG.rsync_ssh_key, 
                            remote_dir = S1GRD1SDV.repository_dir,
                            xk_pull_dag_id = S1GRD1SDV.id + '.sentinel1_gdal',
                            xk_pull_task_id = 'gdal_addo_1',
                            xk_pull_key = 'dstfile',
                            dag=dag)

# Rsync Archive Task for Products
archive_task = RSYNCOperator(task_id="archive_product_task", 
                             host = CFG.rsync_hostname, 
                             remote_usr = CFG.rsync_username,
                             ssh_key_file = CFG.rsync_ssh_key, 
                             remote_dir = S1GRD1SDV.repository_dir,
                             get_inputs_from=download_task.task_id,
                             dag=dag)

# Publish product.zip to GeoServer
publish_task = PythonOperator(task_id="publish_product_task",
                              python_callable=publish_product,
                              op_kwargs={
                                'geoserver_username': CFG.geoserver_username,
                                'geoserver_password': CFG.geoserver_password,
                                'geoserver_rest_endpoint': '{}/oseo/collections/{}/products'.format(CFG.geoserver_rest_url, S1GRD1SDV.geoserver_oseo_collection),
                                'get_inputs_from': metadata_task.task_id,
                              },
                              dag = dag)

search_task >> download_task >> zip_task >> sentinel1_gdal_task >> metadata_task >> upload_task >> archive_task >> publish_task

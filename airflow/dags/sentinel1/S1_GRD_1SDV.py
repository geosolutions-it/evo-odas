import os
import logging
import pprint
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import XCOM_RETURN_KEY

from airflow.operators import PythonOperator
from airflow.operators import RSYNCOperator
from airflow.operators import DHUSSearchOperator
from airflow.operators import DHUSDownloadOperator
from airflow.operators import ZipInspector
from airflow.operators import S1MetadataOperator
from airflow.operators import GDALWarpOperator
from airflow.operators import GDALAddoOperator

from airflow.utils.trigger_rule import TriggerRule

from geoserver_plugin import publish_product
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
    'max_active_runs': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    #
}

print("#######################")
print("Interval: ".format(S1GRD1SDV.dag_schedule_interval))
print("ID: {}".format(S1GRD1SDV.id))
print("DHUS:  {} @ {}, Region: {}".format(CFG.dhus_username, CFG.dhus_url, S1GRD1SDV.dhus_search_bbox) )
print("GeoServer: {} @ {}".format(CFG.geoserver_username, CFG.geoserver_rest_url) )
print("RSYNC: {} @ {} using {}".format(CFG.rsync_username, CFG.rsync_hostname, CFG.rsync_ssh_key))
print("Date: {} / {}".format(S1GRD1SDV.dhus_search_startdate, S1GRD1SDV.dhus_search_enddate))
print("Search: max={}, order_by={}, keywords={}".format(S1GRD1SDV.dhus_filter_max, S1GRD1SDV.dhus_search_orderby,S1GRD1SDV.dhus_search_keywords))
print("Paths:\n  collection_dir={}\n  download_dir={}\n  process_dir={}\n  original_package_upload_dir={}\n  repository_dir={}".format(S1GRD1SDV.collection_dir, S1GRD1SDV.download_dir, S1GRD1SDV.process_dir, S1GRD1SDV.original_package_upload_dir, S1GRD1SDV.repository_dir))
print("Collection:\n  workspace={}\n  layer={}".format(S1GRD1SDV.geoserver_workspace, S1GRD1SDV.geoserver_layer))
print("#######################")

TARGET_SRS = 'EPSG:4326'
TILE_SIZE = 512
OVERWRITE = True

RESAMPLING_METHOD = 'average'
MAX_OVERVIEW_LEVEL = 512

def prepare_band_paths(get_inputs_from, *args, **kwargs):
    """Get Product / Band files path Dictionary from ZipInspector and extract the list of band files """

    task_instance = kwargs['ti']

    # band  number from task name
    task_id = task_instance.task_id
    band_number = int(task_id.split('_')[-1])

    log.info("Getting inputs from: " + get_inputs_from)
    product_bands_dict = task_instance.xcom_pull(task_ids=get_inputs_from, key=XCOM_RETURN_KEY)
    if product_bands_dict is None:
        log.info("No input from ZipInspector. Nothing to do")
        return None

    log.info("Product Band Dictionary: {}".format(pprint.pformat(product_bands_dict)))

    files_path=[]
    for k in product_bands_dict:
        files_path += product_bands_dict[k]

    # Push one of the band paths to XCom
    file_path = files_path[band_number - 1]
    return [file_path]

# DAG definition
dag = DAG(S1GRD1SDV.id, 
          description='DAG for searching, filtering and downloading Sentinel 1 data from DHUS server',
          schedule_interval=S1GRD1SDV.dag_schedule_interval,
          catchup=False,
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

# Rsync Archive Task for Products
archive_task = RSYNCOperator(task_id="upload_original_package",
                             host = CFG.rsync_hostname,
                             remote_usr = CFG.rsync_username,
                             ssh_key_file = CFG.rsync_ssh_key,
                             remote_dir = S1GRD1SDV.original_package_upload_dir,
                             get_inputs_from=download_task.task_id,
                             dag=dag)

# Zip Inspector and Extractor Task
zip_task = ZipInspector(task_id='zip_inspector',
                        extension_to_search='tiff',
                        get_inputs_from=download_task.task_id,
                        dag=dag)

warp_tasks = []
addo_tasks = []
upload_tasks = []
band_paths_tasks = []
for i in range(1, 3):
    band_paths = PythonOperator(task_id="get_band_paths_" + str(i),
         python_callable=prepare_band_paths,
         op_kwargs={
             'get_inputs_from': zip_task.task_id
         },
         dag=dag)
    band_paths_tasks.append(band_paths)

    warp = GDALWarpOperator(
        task_id='gdalwarp_' + str(i),
        target_srs=TARGET_SRS,
        tile_size=TILE_SIZE,
        overwrite=OVERWRITE,
        dstdir=S1GRD1SDV.process_dir,
        get_inputs_from=band_paths.task_id,
        dag=dag
    )
    warp_tasks.append(warp)

    addo = GDALAddoOperator(
        trigger_rule=TriggerRule.ALL_SUCCESS,
        resampling_method=RESAMPLING_METHOD,
        max_overview_level=MAX_OVERVIEW_LEVEL,
        task_id='gdal_addo_' + str(i),
        get_inputs_from=warp.task_id,
        dag=dag
    )
    addo_tasks.append(addo)

    upload = RSYNCOperator(task_id="upload_granule_{}_task".format(str(i)),
                                          host=CFG.rsync_hostname,
                                          remote_usr=CFG.rsync_username,
                                          ssh_key_file=CFG.rsync_ssh_key,
                                          remote_dir=S1GRD1SDV.repository_dir,
                                          get_inputs_from=addo.task_id,
                                          dag=dag)
    upload_tasks.append(upload)

    band_paths.set_upstream(zip_task)
    warp.set_upstream(band_paths)
    addo.set_upstream(warp)
    upload.set_upstream(addo)

# Metadata Extraction task
addo_task_ids = ( task.task_id for task in addo_tasks )
upload_task_ids = ( task.task_id for task in upload_tasks )
metadata_task = S1MetadataOperator(task_id="extract_metadata_task",
                                   product_safe_path=None,
                                   granules_paths=None,
                                   granules_upload_dir=S1GRD1SDV.repository_dir,
                                   working_dir=S1GRD1SDV.process_dir,
                                   original_package_download_base_url=S1GRD1SDV.original_package_download_base_url,
                                   get_inputs_from = {
                                       'download_task_id': download_task.task_id,
                                       'addo_task_ids': addo_task_ids,
                                       'upload_task_ids': upload_task_ids,
                                       'archive_product_task_id' : archive_task.task_id,
                                   },
                                   dag=dag)

# Publish product.zip to GeoServer
publish_task = PythonOperator(task_id="publish_product_task",
                              python_callable=publish_product,
                              op_kwargs={
                                'geoserver_username': CFG.geoserver_username,
                                'geoserver_password': CFG.geoserver_password,
                                'geoserver_rest_endpoint': '{}/oseo/collections/{}/products'.format(CFG.geoserver_rest_url, S1GRD1SDV.geoserver_oseo_collection),                                'get_inputs_from': metadata_task.task_id,
                              },
                              dag = dag)

download_task.set_upstream(search_task)
archive_task.set_upstream(download_task)
zip_task.set_upstream(download_task)
metadata_task.set_upstream(download_task)
metadata_task.set_upstream(archive_task)

for task in upload_tasks:
    metadata_task.set_upstream(task)

publish_task.set_upstream(metadata_task)

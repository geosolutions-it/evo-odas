import logging
from airflow.models import DAG
from airflow.operators import BashOperator, GDALWarpOperator, GDALAddoOperator, GSAddMosaicGranule, RSYNCOperator
from airflow.utils.trigger_rule import TriggerRule
from sentinel1.secrets import geoserver_credentials

log = logging.getLogger(__name__)

# Dag is returned by a factory method
def gdal_processing_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval, working_dir='/var/data/working'):

  TARGET_SRS = 'EPSG:4326'
  TILE_SIZE = 512
  WORKING_DIR = working_dir
  OVERWRITE = True
  
  RESAMPLING_METHOD = 'average'
  MAX_OVERVIEW_LEVEL = 512

  GEOSERVER_REST_URL = 'http://cloudsdi.geo-solutions.it/geoserver/rest'
  GS_USER = geoserver_credentials['username']
  GS_PASSWORD = geoserver_credentials['password']
  STORENAME = 'sentinel1_grd'

  HOST = 'cloudsdi.geo-solutions.it'
  REMOTE_USR = 'airflow'
  SSH_KEY_FILE = '/root/.ssh/id_rsa'
  MOSAIC_PATH = '/efs/geoserver_data/coverages/sentinel/sentinel1/grd'

  dag = DAG(
    '%s.%s' % (parent_dag_name, child_dag_name),
    schedule_interval=schedule_interval,
    start_date=start_date,
  )

  for i in range(1, 3):
    warp = GDALWarpOperator(
        target_srs = TARGET_SRS,
        tile_size = TILE_SIZE,
        overwrite = OVERWRITE,
        task_id='gdalwarp_' + str(i),
        xk_pull_dag_id = parent_dag_name,
        xk_pull_task_id='zip_inspector',
        xk_pull_key_srcfile='img_zip_abs_path_' + str(i),
        dstdir=WORKING_DIR,
        dag = dag
    )

    addo = GDALAddoOperator(
        trigger_rule=TriggerRule.ALL_SUCCESS,
        resampling_method = RESAMPLING_METHOD,
        max_overview_level = MAX_OVERVIEW_LEVEL,
        task_id = 'gdal_addo_' + str(i),
        xk_pull_task_id='gdalwarp_' + str(i),
        xk_pull_key_srcfile='dstfile',
        dag = dag
    )

    warp >> addo
    """
    transfer = RSYNCOperator(
        host = HOST,
        remote_usr = REMOTE_USR,
        ssh_key_file = SSH_KEY_FILE,
        remote_dir = MOSAIC_PATH,
        working_dir = WORKING_DIR,
        index = i,
        task_id = 'rsync_' + str(i),
        dag = dag
    )

  
    add_granule = GSAddMosaicGranule(
        geoserver_rest_url = GEOSERVER_REST_URL,
        gs_user = GS_USER,
        gs_password = GS_PASSWORD,
        imagemosaic_storename = STORENAME,
        mosaic_path = MOSAIC_PATH,
        index = i,
        task_id = 'gs_add_mosaic_granule' + str(i),
        dag = dag
    )
  """
  #warp >> addo >> transfer >> add_granule


  return dag

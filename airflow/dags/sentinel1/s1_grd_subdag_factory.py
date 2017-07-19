from airflow.models import DAG
from airflow.operators import BashOperator, GDALWarpOperator, GDALAddoOperator, GSAddMosaicGranule, RSYNCOperator
import logging

log = logging.getLogger(__name__)

# Dag is returned by a factory method
def gdal_processing_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):

  TARGET_SRS = 'EPSG:4326'
  TILE_SIZE = 512
  WORKING_DIR = '/home/fds/Desktop/work/configurations/mosaic_sentinel1_test'
  OVERWRITE = True

  RESAMPLING_METHOD = 'average'
  MAX_OVERVIEW_LEVEL = 512

  GEOSERVER_REST_URL = 'http://cloudsdi.geo-solutions.it/geoserver/rest'
  GS_USER = 'admin'
  GS_PASSWORD = '******'
  STORENAME = 'sentinel1_grd'

  HOST = 'cloudsdi.geo-solutions.it'
  REMOTE_USR = 'airflow'
  SSH_KEY_FILE = '/root/.ssh/id_rsa'
  MOSAIC_PATH = '/efs/geoserver_data/coverages/sentinel/sentinel1/grd'
  WORKING_DIR = '/tmp'

  dag = DAG(
    '%s.%s' % (parent_dag_name, child_dag_name),
    schedule_interval=schedule_interval,
    start_date=start_date,
  )

  for i in range(1, 3):
    warp = GDALWarpOperator(
        target_srs = TARGET_SRS,
        tile_size = TILE_SIZE,
        working_dir = WORKING_DIR,
        overwrite = OVERWRITE,
        index = i,
        task_id ='gdal_warp_' + str(i),
        dag = dag,
        main_dag_name = parent_dag_name
    )

    addo = GDALAddoOperator(
        resampling_method = RESAMPLING_METHOD,
        max_overview_level = MAX_OVERVIEW_LEVEL,
        index = i,
        task_id = 'gdal_addo_' + str(i),
        dag = dag
    )

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

  """
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
  warp >> addo

  return dag

from datetime import datetime
from getpass import getuser

from airflow import DAG
from airflow.operators import DownloadSceneList
from airflow.operators import ExtractSceneList
from airflow.operators import UpdateSceneList

import config as CFG
import config.landsat8 as LANDSAT8

landsat8_scene_list = DAG(
    LANDSAT8.id + '_Scene_List',
    description='DAG for downloading, extracting, and importing scene_list.gz '
                'into postgres db',
    default_args={
        "start_date": datetime(2017, 1, 1),
        "owner": getuser(),
        "depends_on_past": False,
        "provide_context": True,
        "email": ["xyz@xyz.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,  # TODO: change back to 1
        "max_threads": 1,
        "download_dir": LANDSAT8.download_dir,
        "download_url": LANDSAT8.download_url,
    },
    dagrun_timeout=LANDSAT8.dagrun_timeout,
    schedule_interval=LANDSAT8.dag_schedule_interval,
    catchup=False
)

# more info on Landsat products on AWS at:
# https://aws.amazon.com/public-datasets/landsat/
download_scene_list_gz = DownloadSceneList(
    task_id='download_scene_list_gz',
    dag=landsat8_scene_list
)

extract_scene_list = ExtractSceneList(
    task_id='extract_scene_list',
    dag=landsat8_scene_list
)

update_scene_list_db = UpdateSceneList(
    task_id='update_scene_list',
    pg_dbname=CFG.landsat8_postgresql_credentials['dbname'],
    pg_hostname=CFG.landsat8_postgresql_credentials['hostname'],
    pg_port=CFG.landsat8_postgresql_credentials['port'],
    pg_username=CFG.landsat8_postgresql_credentials['username'],
    pg_password=CFG.landsat8_postgresql_credentials['password'],
    dag=landsat8_scene_list
)

download_scene_list_gz.set_downstream(extract_scene_list)
extract_scene_list.set_downstream(update_scene_list_db)

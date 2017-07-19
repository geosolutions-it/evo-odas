from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators import BaseOperator, BashOperator, DownloadSceneList, ExtractSceneList, UpdateSceneList
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


scene_list_args = {
    ##################################################
    # General configuration
    ##################################################
    'start_date': datetime(2017, 1, 1),
    'owner': 'airflow',
    'depends_on_past': False,
    'provide_context': True,
    'email': ['xyz@xyz.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'max_threads': 1,
    #################################################
    # DownloadSceneList
    ################################################
    'download_url':'http://landsat-pds.s3.amazonaws.com/c1/L8/scene_list.gz', 
    'download_dir': '/home/moataz/airflow/data/downloads/'

}


# DAG definition
landsat8_scene_list = DAG('Landsat8_Scene_List', description='DAG for downloading, extracting, and importing scene_list.gz into postgres db',
          default_args=scene_list_args,
          dagrun_timeout=timedelta(hours=1),
          schedule_interval=timedelta(days=1),
          catchup=False)


download_scene_list_gz = DownloadSceneList(task_id= 'download_scene_list_gz_task', dag = landsat8_scene_list)

extract_scene_list = ExtractSceneList(task_id = 'extract_scene_list_task', dag = landsat8_scene_list )

update_scene_list_db = UpdateSceneList(task_id = 'update_scene_list_task', dag = landsat8_scene_list )

download_scene_list_gz >> extract_scene_list >> update_scene_list_db

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators import BaseOperator, BashOperator, DownloadSceneList, ExtractSceneList, UpdateSceneList
from landsat8.secrets import postgresql_credentials
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

##################################################
# General and shared configuration between tasks
##################################################
update_scene_list_default_args = {
    'start_date': datetime(2017, 1, 1),
    'owner': 'airflow',
    'depends_on_past': False,
    'provide_context': True,
    'email': ['xyz@xyz.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'max_threads': 1,
    'download_dir': '/var/data/download/'
}

######################################################
# Task specific configuration
######################################################
download_scene_list_args = {
		'download_url':'http://landsat-pds.s3.amazonaws.com/c1/L8/scene_list.gz'
}

update_scene_list_args = {
		'psql_dbname' : postgresql_credentials['dbname'],
		'psql_hostname' : postgresql_credentials['hostname'],
		'psql_port' : postgresql_credentials['port'],
		'psql_username' : postgresql_credentials['username']
}

# DAG definition
landsat8_scene_list = DAG('Landsat8_Scene_List', 
		description='DAG for downloading, extracting, and importing scene_list.gz into postgres db',
		default_args=update_scene_list_default_args,
		dagrun_timeout=timedelta(hours=1),
		schedule_interval=timedelta(days=1),
		catchup=False)

# Tasks difinition
download_scene_list_gz = DownloadSceneList(
		task_id= 'download_scene_list_gz_task', 
		download_url = download_scene_list_args['download_url'], 
		#download_dir = download_scene_list_args['download_dir'], 
		dag = landsat8_scene_list)

extract_scene_list = ExtractSceneList(
		task_id = 'extract_scene_list_task', 
		#download_dir = extract_scene_list_args['download_dir'] , 
		dag = landsat8_scene_list)

update_scene_list_db = UpdateSceneList(
		task_id = 'update_scene_list_task', 
		psql_dbname = update_scene_list_args['psql_dbname'] , 
		psql_hostname = update_scene_list_args['psql_hostname'], 
		psql_port = update_scene_list_args['psql_port'], 
		psql_username = update_scene_list_args['psql_username'] , 
		dag = landsat8_scene_list )

download_scene_list_gz >> extract_scene_list >> update_scene_list_db

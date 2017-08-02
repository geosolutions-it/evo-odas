import logging
import pprint
import urllib
import gzip
import psycopg2
from datetime import datetime, timedelta
from airflow.operators import BaseOperator, BashOperator 
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


log = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)

class DownloadSceneList(BaseOperator):

    @apply_defaults
    def __init__(self, download_url, download_dir, download_timeout=timedelta(hours=1),*args, **kwargs):
        self.download_url = download_url
        self.download_dir = download_dir
        log.info('-------------------- DownloadSceneList ------------')
        super(DownloadSceneList, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('-------------------- DownloadSceneList Execute------------')
        urllib.urlretrieve(self.download_url,self.download_dir+"scene_list.gz")
        context['task_instance'].xcom_push(key='scene_list_gz_path', value=str(self.download_dir)+"scene_list.gz")
        return True

class ExtractSceneList(BaseOperator):

    @apply_defaults
    def __init__(self, download_dir,*args, **kwargs):
        log.info('-------------------- ExtractSceneList ------------')
        self.download_dir = download_dir
        super(ExtractSceneList, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('-------------------- ExtractSceneList Execute------------')
        scene_list_gz_path = context['task_instance'].xcom_pull('download_scene_list_gz_task', key='scene_list_gz_path')
        with gzip.open(scene_list_gz_path, 'rb') as f:
             file_content = f.read()
        outF = file(self.download_dir+"scene_list.csv", 'wb')
        outF.write(file_content)
        outF.close()
        context['task_instance'].xcom_push(key='scene_list_csv_path', value=str(self.download_dir)+"scene_list.csv")
        return True

class UpdateSceneList(BaseOperator):

    @apply_defaults
    def __init__(self, pg_dbname, pg_hostname, pg_port, pg_username, pg_password,*args, **kwargs):
        log.info('-------------------- UpdateSceneList ------------')
        self.pg_dbname = pg_dbname
        self.pg_hostname = pg_hostname
        self.pg_port = pg_port
        self.pg_username = pg_username
        self.pg_password = pg_password
        super(UpdateSceneList, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('-------------------- UpdateSceneList Execute------------')
        scene_list_csv_path = context['task_instance'].xcom_pull('extract_scene_list_task', key='scene_list_csv_path')
        
        delete_first_line_cmd = "tail -n +2 " + scene_list_csv_path + "> " + scene_list_csv_path+".tmp && mv "+ scene_list_csv_path +".tmp  " + scene_list_csv_path
        delete_line_operator = BashOperator(task_id='Delete_first_line_OP', bash_command = delete_first_line_cmd)
        delete_line_operator.execute(context)

        db = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(self.pg_dbname, self.pg_username, self.pg_hostname, self.pg_password))
        cursor = db.cursor()
        cursor.execute("delete from scene_list")
        db.commit()

        fo = open(scene_list_csv_path, 'r')	
        cursor.copy_from(fo, 'scene_list',sep=',')
        db.commit()

        fo.close()
        return True

class LANDSAT8DBPlugin(AirflowPlugin):
    name = "landsat8db_plugin"
    operators = [DownloadSceneList, ExtractSceneList, UpdateSceneList]


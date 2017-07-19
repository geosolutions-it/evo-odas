import logging
import pprint
import urllib
import gzip
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
    def __init__(self, *args, **kwargs):
        log.info('-------------------- UpdateSceneList ------------')
        super(UpdateSceneList, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('-------------------- UpdateSceneList Execute------------')
        scene_list_csv_path = context['task_instance'].xcom_pull('extract_scene_list_task', key='scene_list_csv_path')

        postgres_delete_command = r"echo '\x \\ delete FROM scene_list;' | psql"
        delete_op = BashOperator(task_id='DeleteOP_psql', bash_command=postgres_delete_command)
        delete_op.execute(context)

        postgres_import_command = "psql -c \"" + "\copy scene_list FROM '" +scene_list_csv_path + r"' delimiter ',' csv header""
        import_op = BashOperator(task_id='ImportOP_psql', bash_command=postgres_import_command)
        import_op.execute(context)

        return True

class LANDSAT8DBPlugin(AirflowPlugin):
    name = "landsat8db_plugin"
    operators = [DownloadSceneList, ExtractSceneList, UpdateSceneList]


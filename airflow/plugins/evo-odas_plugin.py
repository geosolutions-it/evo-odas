from airflow.operators import BashOperator
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from zipfile import ZipFile
import xcom_keys as xk
import logging

log = logging.getLogger(__name__)

class ZipInspector(BaseOperator):

    @apply_defaults
    def __init__(self, extension_to_search, *args, **kwargs):
        self.substring = extension_to_search
        log.info('--------------------GDAL_PLUGIN Zip inspector------------')
        super(ZipInspector, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('**** Inside execute ****')
        task_instance = context['task_instance']
        log.info("ZipInspector Operator params list")
        log.info('Substring to search: %s', self.substring)

        downloaded = task_instance.xcom_pull(task_ids='dhus_download_task', key='downloaded_products')
        log.info("Downloaded products: " + pprint.pformat(downloaded))
        zip_filename = None
        # TODO: Only handling one at a time
        for k in downloaded:
            zip_filename = downloaded[k]['path']
            break
        if zip_filename is None:
            log.error("No Zip file retrived via XCom. Nothing to do")
            raise TypeError

        log.info('Zip filename: %s', zip_filename)
        zip = ZipFile(zip_filename)
        counter = 0;
        for file in zip.namelist():
            filename = zip.getinfo(file).filename
            if self.substring in filename:
                counter = counter + 1
                raster_vsizip = "/vsizip/" + zip_filename + "/" + filename
                log.info(str(counter) + ") '" + raster_vsizip + "'")
                task_instance.xcom_push(key=xk.IMAGE_ZIP_ABS_PATH_PREFIX_XCOM_KEY + str(counter), value=raster_vsizip)
        if counter == 0:
            log.info("No files found in the zip archive...")
            return True
        else:
            return False



class RSYNCOperator(BaseOperator):

    @apply_defaults
    def __init__(self, host, remote_usr, ssh_key_file, remote_dir, files_to_upload=None, xk_pull_dag_id=None, xk_pull_task_id=None, xk_pull_key=None, *args, **kwargs):
        self.host = host
        self.remote_usr = remote_usr
        self.ssh_key_file = ssh_key_file
        self.remote_dir = remote_dir
        self.files_to_upload = files_to_upload
        self.xk_pull_dag_id = xk_pull_dag_id
        self.xk_pull_task_id = xk_pull_task_id
        self.xk_pull_key = xk_pull_key
        log.info('--------------------RSYNCOperator ------------')
        super(RSYNCOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        task_instance = context['task_instance']

        # init XCom parameters
        if self.xk_pull_dag_id is None:
            self.xk_pull_dag_id = context['dag'].dag_id
        # check input file path passed otherwise look for it in XCom
        if self.files_to_upload is None:
            log.info('xk_pull_dag_id: %s', self.xk_pull_dag_id)
            log.info('xk_pull_task_id: %s', self.xk_pull_task_id)
            log.info('xk_pull_key: %s', self.xk_pull_key)
            files_str = task_instance.xcom_pull(task_ids=self.xk_pull_task_id, key=self.xk_pull_key, dag_id=self.xk_pull_dag_id)
        else:
            files_str = ""
            for f in self.files_to_upload:
                files_str += " " + f

        bash_command = 'rsync -avHPze "ssh -i ' + self.ssh_key_file + ' -o StrictHostKeyChecking=no" ' + files_str + ' ' + self.remote_usr + '@' + self.host + ':' + self.remote_dir
        bo = BashOperator(task_id='bash_operator_rsync_', bash_command=bash_command)
        bo.execute(context)

class EVOODASPlugin(AirflowPlugin):
    name = "RSYNC_plugin"
    operators = [RSYNCOperator, ZipInspector]

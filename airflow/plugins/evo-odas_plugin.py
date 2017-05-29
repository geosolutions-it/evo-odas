import logging
from airflow.operators import BashOperator
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from zipfile import ZipFile
import config.xcom_keys as xk

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
        zip_filename = task_instance.xcom_pull(task_ids='download', key=xk.PACKAGE_LOCATION_XCOM_KEY)

        log.info("ZipInspector Operator params list")
        log.info('Zip filename: %s', zip_filename)
        log.info('Substring to search: %s', self.substring)
        if zip_filename == None:
                raise TypeError("Error while opening the zip file... is the '" + xk.PACKAGE_LOCATION_XCOM_KEY + "' xcom correctly pushed by the previous task?")

        zip=ZipFile(zip_filename)
        counter = 0;
        for file in zip.namelist():
            filename = zip.getinfo(file).filename
            if self.substring in filename:
                counter = counter+1
                raster_vsizip = "/vsizip/" + zip_filename + "/" + filename
                log.info(str(counter) + ") '" + raster_vsizip + "'")
                task_instance.xcom_push(key=xk.IMAGE_ZIP_ABS_PATH_PREFIX_XCOM_KEY + str(counter), value=raster_vsizip)
        if counter == 0:
            log.info("No files found in the zip archive...")


class RSYNCOperator(BaseOperator):

    @apply_defaults
    def __init__(self, host, remote_usr, ssh_key_file, remote_dir, working_dir, index, *args, **kwargs):
        self.host = host
        self.remote_usr = remote_usr
        self.ssh_key_file = ssh_key_file
        self.remote_dir = remote_dir
        self.working_dir = working_dir
        self.index = index
        log.info('--------------------RSYNCOperator Zip inspector------------')

    def execute(self, context):
        task_instance = context['task_instance']
        zip_filename = task_instance.xcom_pull(task_ids='download', key=xk.PACKAGE_LOCATION_XCOM_KEY + self.index)

        splitted_filename = zip_filename.split('_')
        filename_to_upload = ""

        file_to_upload = self.working_dir + "/" + zip_filename
        remote_file = self.remote_dir + "/granule_" + splitted_filename[2] + "_" + splitted_filename[4] + ".tif"

        bash_command = 'rsync -e "ssh -i ' + self.ssh_key_file + ' -o StrictHostKeyChecking=no" -avHPe -z "' + file_to_upload + '" ' + self.remote_usr + '@' + self.host + ':' + remote_file
        bo = BashOperator(task_id='bash_operator_rsync_' + self.index, bash_command=bash_command)
        bo.execute(context)

class EVOODASPlugin(AirflowPlugin):
    name = "RSYNC_plugin"
    operators = [RSYNCOperator, ZipInspector]

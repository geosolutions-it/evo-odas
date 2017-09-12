import logging
import pprint
import os 
import fnmatch
from airflow.operators import BashOperator
from airflow.operators import BaseOperator
from airflow.operators import PythonOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import XCOM_RETURN_KEY

from zipfile import ZipFile
import config.xcom_keys as xk
#from config import xcom_keys as xk
from sentinel1.utils.ssat1_metadata import create_procuct_zip

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
    def __init__(self, 
                 host, 
                 remote_usr,
                 ssh_key_file,
                 remote_dir,
                 files_to_upload=None,
                 get_inputs_from=None,
                 xk_pull_dag_id=None,
                 xk_pull_task_id=None,
                 xk_pull_key=None,
                 *args, **kwargs):
        self.host = host
        self.remote_usr = remote_usr
        self.ssh_key_file = ssh_key_file
        self.remote_dir = remote_dir
        self.files_to_upload = files_to_upload
        self.xk_pull_dag_id = xk_pull_dag_id
        self.xk_pull_task_id = xk_pull_task_id
        self.xk_pull_key = xk_pull_key
        self.get_inputs_from = get_inputs_from

        log.info('--------------------RSYNCOperator ------------')
        super(RSYNCOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info(context)
        log.info("###########")
        log.info("## RSYNC ##")
        log.info('Host: %s', self.host)
        log.info('User: %s', self.remote_usr)
        log.info('Remote dir: %s', self.remote_dir)
        log.info('SSH Key: %s', self.ssh_key_file)
        
        # check default XCOM key in task_id 'get_inputs_from'
        files_str = ""
        if self.get_inputs_from != None:
            files_dict = context['task_instance'].xcom_pull(task_ids=self.get_inputs_from, key=XCOM_RETURN_KEY)

            # stop processing if there are no products
            if files_dict is None:
                log.info("Nothing to process.")
                return

            for f in files_dict.keys():
                files_str += " " + f
            log.info("Retrieving input from task_id '{}'' and key '{}'".format(self.get_inputs_from, XCOM_RETURN_KEY))
        else:
            # init XCom parameters
            if self.xk_pull_dag_id is None:
                self.xk_pull_dag_id = context['dag'].dag_id

            # check input file path passed otherwise look for it in XCom
            if self.files_to_upload is None:
                log.info('xk_pull_dag_id: %s', self.xk_pull_dag_id)
                log.info('xk_pull_task_id: %s', self.xk_pull_task_id)
                log.info('xk_pull_key: %s', self.xk_pull_key)
                files_str = context['task_instance'].xcom_pull(task_ids=self.xk_pull_task_id, key=self.xk_pull_key, dag_id=self.xk_pull_dag_id)
            else:
                for f in self.files_to_upload:
                    files_str += " " + f

        bash_command = 'rsync -avHPze "ssh -i ' + self.ssh_key_file + ' -o StrictHostKeyChecking=no" ' + files_str + ' ' + self.remote_usr + '@' + self.host + ':' + self.remote_dir
        bo = BashOperator(task_id='bash_operator_rsync_', bash_command=bash_command)
        bo.execute(context)

class S1MetadataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, product_safe_path, granules_paths, granules_upload_dir, xk_pull_dag_id=None, xk_pull_task_id=None, xk_pull_key_safe=None, xk_pull_key_granules_paths=None, xk_push_key=None,
                 *args, **kwargs):
        self.product_safe_path = product_safe_path
        self.granules_paths = granules_paths
        self.granules_upload_dir = granules_upload_dir
        self.xk_pull_dag_id = xk_pull_dag_id
        self.xk_pull_task_id = xk_pull_task_id
        self.xk_pull_key_safe = xk_pull_key_safe
        self.xk_pull_key_granules_paths = xk_pull_key_granules_paths
        self.xk_push_key = xk_push_key

        super(S1MetadataOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('--------------------S1Metadata_PLUGIN running------------')
        task_instance = context['task_instance']

        log.info("""
            product_safe_path: {}
            granules_paths: {}
            granules_upload_dir: {}
            xk_pull_task_id: {}
            xk_pull_dag_id: {}
            xk_pull_key_safe: {}
            xk_pull_key_granules_paths: {}
            xk_push_key: {}
            """.format(
            self.product_safe_path,
            self.granules_paths,
            self.granules_upload_dir,
            self.xk_pull_task_id,
            self.xk_pull_dag_id,
            self.xk_pull_key_safe,
            self.xk_pull_key_granules_paths,
            self.xk_push_key,
            )
        )
        # init XCom parameters
        if self.xk_pull_dag_id is None:
            self.xk_pull_dag_id = context['dag'].dag_id
        # check input file path passed otherwise look for it in XCom
        if self.product_safe_path is not None:
            product_safe_path = self.product_safe_path
        else:
            if self.xk_pull_key_safe is None:
                self.xk_pull_key_safe = 'product_safe_path'
            # log.info('XCom Pull: task_ids: {} key: {} dag_id: {}'.format(self.xk_pull_task_id, self.xk_pull_key_safe, self.xk_pull_dag_id))
            downloaded = task_instance.xcom_pull(task_ids='dhus_download_task', key='downloaded_products', dag_id=self.xk_pull_dag_id)
            zip_filename = None
            for k in downloaded:
                zip_filename = downloaded[k]['path']
                break
        if zip_filename is None:
            log.error("No Zip file retrived via XCom")
            raise TypeError
        log.info('product_safe_path: %s', zip_filename)


        if self.granules_paths is not None:
            granules_paths = self.granules_paths
        else:
            if self.xk_pull_key_granules_paths is None:
                self.xk_pull_key_granules_paths = 'granules_paths'
            # log.info('XCom Pull: task_ids: {} key: {} dag_id: {}'.format(self.xk_pull_task_id, self.xk_pull_key_safe, self.xk_pull_dag_id))
            self.xk_pull_dag_id += '.sentinel1_gdal'
            log.info('xk_pull_dag_id: %s', self.xk_pull_dag_id)
            granules_paths = []
            granule_path = task_instance.xcom_pull(task_ids='gdal_addo_1', key='dstfile', dag_id=self.xk_pull_dag_id)
            granules_paths.append(granule_path)
            granule_path = task_instance.xcom_pull(task_ids='gdal_addo_2', key='dstfile', dag_id=self.xk_pull_dag_id)
            granules_paths.append(granule_path)

        if len(granules_paths) == 0:
            log.error("No Granules path retrived via XCom")
            raise TypeError
        log.info('granules_paths: %s', granules_paths)

        # push output path to XCom
        #if self.xk_push_key is None:
        #    self.xk_push_key = 'dstfile'
        #task_instance.xcom_push(key='dstfile', value=dstfile)

        po = PythonOperator(
            task_id="s1_metadata_dictionary_creation",
            python_callable=create_procuct_zip,
            op_kwargs={
                'sentinel1_product_zip_path': zip_filename,
                'granules_paths': granules_paths,
                'granules_upload_dir':self.granules_upload_dir,
            }
        )
        po.execute(context)

class MoveFilesOperator(BaseOperator):
    @apply_defaults
    def __init__(self, src_dir, dst_dir, filter, *args, **kwargs):
        super(MoveFilesOperator, self).__init__(*args, **kwargs)
        self.src_dir = src_dir
        self.dst_dir = dst_dir
        self.filter = filter
        log.info('--------------------MoveFilesOperator------------')

    def execute(self, context):
        log.info('\nsrc_dir={}\ndst_dir={}\nfilter={}'.format(self.src_dir, self.dst_dir, self.filter))
        filenames = fnmatch.filter(os.listdir(self.src_dir), self.filter)
        for filename in filenames:
            filepath = os.path.join(self.src_dir, filename)
            if not os.path.exists(self.dst_dir):
                log.info("Creating directory {}".format(self.dst_dir))
                os.makedirs(self.dst_dir)
            dst_filename = os.path.join(self.dst_dir, os.path.basename(filename))
            log.info("Moving {} to {}".format(filepath, dst_filename))
            #os.rename(filepath, dst_filename)

class EVOODASPlugin(AirflowPlugin):
    name = "RSYNC_plugin"
    operators = [RSYNCOperator, ZipInspector, S1MetadataOperator, MoveFilesOperator]

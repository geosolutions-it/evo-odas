import logging
import pprint
import os
import sys
import six
import fnmatch
from airflow.operators import BashOperator
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import XCOM_RETURN_KEY

try:
    from osgeo import gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules, install gdal with python bindings')

from zipfile import ZipFile
import config.xcom_keys as xk
from geoserver_plugin import create_owslinks_dict

log = logging.getLogger(__name__)


class ZipInspector(BaseOperator):
    """ ZipInspector takes list of downloaded products and goes through the downloaded product's zipfiles and searches for a defined extension (.tiff)  

    Args:
        extension_to_search (str): image extension to search for 
        get_inputs_from (str): task_id used to fetch input files from XCom (as a list of products)

    Returns:
        dict: keys are zipfiles and values are lists containing virtual paths 
    """
    @apply_defaults
    def __init__(self, extension_to_search,  get_inputs_from=None, *args, **kwargs):
        self.substring = extension_to_search
        self.get_inputs_from = get_inputs_from
        log.info('--------------------GDAL_PLUGIN Zip inspector------------')
        super(ZipInspector, self).__init__(*args, **kwargs)

    def execute(self, context):
        zip_files=list()
        if self.get_inputs_from != None:
            zip_files = context['task_instance'].xcom_pull(task_ids=self.get_inputs_from, key=XCOM_RETURN_KEY)
        else:
            #legacy 
            log.info('**** Inside execute ****')
            log.info("ZipInspector Operator params list")
            log.info('Substring to search: %s', self.substring)

            downloaded = context['task_instance'].xcom_pull(task_ids='dhus_download_task', key='downloaded_products')
            zip_files = downloaded.keys()
        
        # stop processing if there are no products
        if zip_files is None or len(zip_files)==0:
            log.info("Nothing to process.")
            return

        return_dict=dict()
        log.info("Processing {} ZIP files:\n{} ".format(len(zip_files),pprint.pformat(zip_files)))
        for zip_file in zip_files:
            log.info("Processing {}..".format(zip_file))
            counter = 0;
            vsi_paths=list()
            zip = ZipFile(zip_file)
            for file in zip.namelist():
                filename = zip.getinfo(file).filename
                if self.substring in filename:
                    counter = counter + 1
                    raster_vsizip = "/vsizip/" + zip_file + "/" + filename
                    vsi_paths.append(raster_vsizip)
                    log.info(str(counter) + ") '" + raster_vsizip + "'")
                    context['task_instance'].xcom_push(key=xk.IMAGE_ZIP_ABS_PATH_PREFIX_XCOM_KEY + str(counter), value=raster_vsizip)
            return_dict[zip_file]=vsi_paths

        return return_dict

class RSYNCOperator(BaseOperator):
    """ RSYNCOperator is using rsync command line to upload files remotely using ssh keys

    Args:
        host (str): hostname/ip of the remote machine
        remote_usr (str): username of the remote account/machine
        ssh_key_file (str): path to the ssh key file
        remote_dir (str): remote directory to receive the uploaded files
        get_inputs_from (str): task_id used to fetch input file(s) from XCom 

    Returns:
        list: list of paths for the uploaded/moved files  
    """

    @apply_defaults
    def __init__(self, 
                 host, 
                 remote_usr,
                 ssh_key_file,
                 remote_dir,
                 get_inputs_from=None,
                 *args, **kwargs):
        self.host = host
        self.remote_usr = remote_usr
        self.ssh_key_file = ssh_key_file
        self.remote_dir = remote_dir
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
        files = context['task_instance'].xcom_pull(task_ids=self.get_inputs_from, key=XCOM_RETURN_KEY)

        # stop processing if there are no products
        if files is None:
            log.info("Nothing to process.")
            return

        if isinstance(files, six.string_types):
            files_str = files
        else:
            for f in files:
                files_str += " " + f
            log.info("Retrieving input from task_id '{}'' and key '{}'".format(self.get_inputs_from, XCOM_RETURN_KEY))

        bash_command = 'rsync -avHPze "ssh -i ' + self.ssh_key_file + ' -o StrictHostKeyChecking=no" ' + files_str + ' ' + self.remote_usr + '@' + self.host + ':' + self.remote_dir
        bo = BashOperator(task_id='bash_operator_rsync_', bash_command=bash_command)
        bo.execute(context)

        # construct list of filenames uploaded to remote host
        files_list = files_str.split()
        filenames_list = list(os.path.join(self.remote_dir, os.path.basename(path)) for path in files_list)
        log.info("Uploaded files: {}".format(pprint.pformat(files_list)))
        return filenames_list

class MoveFilesOperator(BaseOperator):
    """ MoveFilesOperator is moving files according to a filter to be applied on the file's names

    Args:
        src_dir (str): source directory to look for files inside
        dst_dir (str): destination directory to send files to
        filter (str): expression to filter the files accordingly

    Returns:
        True
    """
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
        if filenames is None or len(filenames) == 0:
            log.info("No files to move.")
            return
        for filename in filenames:
            filepath = os.path.join(self.src_dir, filename)
            if not os.path.exists(self.dst_dir):
                log.info("Creating directory {}".format(self.dst_dir))
                os.makedirs(self.dst_dir)
            dst_filename = os.path.join(self.dst_dir, os.path.basename(filename))
            log.info("Moving {} to {}".format(filepath, dst_filename))
            #os.rename(filepath, dst_filename)
        return True

class EVOODASPlugin(AirflowPlugin):
    name = "RSYNC_plugin"
    operators = [RSYNCOperator, ZipInspector, MoveFilesOperator]

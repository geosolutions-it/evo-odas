import os
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import XCOM_RETURN_KEY

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

class UtilsPlugin(AirflowPlugin):
    name = "Utils_plugin"
    operators = [MoveFilesOperator]
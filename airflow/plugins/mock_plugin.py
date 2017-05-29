import logging

from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import config.xcom_keys as xk

log = logging.getLogger(__name__)

class MockDownload(BaseOperator):

    @apply_defaults
    def __init__(self, downloaded_path, *args, **kwargs):
        log.info('--------------------MOCK_PLUGIN mock download------------')
        self.downloaded_path = downloaded_path
        super(MockDownload, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Executing MockDownload...")
        log.info("the downloaded path is '" + self.downloaded_path + "'")
        task_instance = context['task_instance']
        task_instance.xcom_push(key=xk.PACKAGE_LOCATION_XCOM_KEY, value=self.downloaded_path)
        log.info("...MockDownload executed!")

class MockPlugin(AirflowPlugin):
    name = "Mock_plugin"
    operators = [MockDownload]

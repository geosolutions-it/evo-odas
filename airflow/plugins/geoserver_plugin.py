import logging
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import config.xcom_keys as xk
from geoserver.catalog import Catalog

import sys
reload(sys)
sys.setdefaultencoding('utf8')

log = logging.getLogger(__name__)

class GSAddMosaicGranule(BaseOperator):

    @apply_defaults
    def __init__(self, geoserver_rest_url, gs_user, gs_password, imagemosaic_storename, mosaic_path, index, *args, **kwargs):
        self.catalog = Catalog(geoserver_rest_url, gs_user, gs_password)
        self.store_name = imagemosaic_storename
        self.mosaic_path = mosaic_path
        self.index = index
        log.info('--------------------GDAL_PLUGIN Add granule------------')
        super(GSAddMosaicGranule, self).__init__(*args, **kwargs)

    def execute(self, context):
        task_instance = context['task_instance']
        granule = task_instance.xcom_pull('rsync_' + str(self.index), key=xk.GRANULE_TO_UPLOAD_PREFIX + str(self.index))
        log.info("GSAddMosaicGranule params list")
        log.info('Mosaic granule: %s', granule)
        store = self.catalog.get_store(self.store_name)
        granule = 'file://' + self.mosaic_path + '/' + granule
        log.info(granule)
        self.catalog.harvest_externalgranule(granule, store)

class GDALPlugin(AirflowPlugin):
    name = "GeoServer_plugin"
    operators = [GSAddMosaicGranule]

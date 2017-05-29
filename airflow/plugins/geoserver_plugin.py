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
    def __init__(self, geoserver_rest_url, gs_user, gs_password, imagemosaic_storename_prefix, index, *args, **kwargs):
        self.catalog = Catalog(geoserver_rest_url, gs_user, gs_password)
        self.store_name_prefix = imagemosaic_storename_prefix
        self.index = index
        log.info('--------------------GDAL_PLUGIN Add granule------------')
        super(GSAddMosaicGranule, self).__init__(*args, **kwargs)

    def execute(self, context):
        task_instance = context['task_instance']
        granule_abs_path = task_instance.xcom_pull('gdal_warp_' + str(self.index), key=xk.WORKDIR_IMG_NAME_PREFIX_XCOM_KEY + str(self.index))
        log.info("GSAddMosaicGranule params list")
        log.info('Mosaic granule: %s', granule_abs_path)
        store = self.catalog.get_store(self.store_name_prefix)
        self.catalog.harvest_externalgranule('file://' + granule_abs_path, store)

class GDALPlugin(AirflowPlugin):
    name = "GeoServer_plugin"
    operators = [GSAddMosaicGranule]

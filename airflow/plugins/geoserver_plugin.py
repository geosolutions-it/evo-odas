import logging

from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from geoserver.catalog import Catalog

import sys
reload(sys)
sys.setdefaultencoding('utf8')

log = logging.getLogger(__name__)

class GSAddMosaicGranule(BaseOperator):

    @apply_defaults
    def __init__(self, granule_abs_path, geoserver_rest_url, gs_user, gs_password, imagemosaic_storename, *args, **kwargs):
        self.granule_abs_path = granule_abs_path
        self.catalog = Catalog(geoserver_rest_url, gs_user, gs_password)
        self.store_name = imagemosaic_storename
        super(GSAddMosaicGranule, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("GSAddMosaicGranule params list")
        log.info('Mosaic granule: %s', self.granule_abs_path)
        store = self.catalog.get_store(self.store_name)
        self.catalog.harvest_externalgranule('file://' + self.granule_abs_path, store)

class GDALPlugin(AirflowPlugin):
    name = "GeoServer_plugin"
    operators = [GSAddMosaicGranule]

from datetime import timedelta
from itertools import chain
import logging
import os
import psycopg2
import urllib

from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class Landsat8SearchOperator(BaseOperator):

    @apply_defaults
    def __init__(self, area, cloud_coverage, db_credentials, *args, **kwargs):
        super(Landsat8SearchOperator, self).__init__(*args, **kwargs)
        self.area = area
        self.cloud_coverage = cloud_coverage
        self.db_credentials = dict(db_credentials)

    def execute(self, context):
        connection = psycopg2.connect(
            dbname=self.db_credentials["dbname"],
            user=self.db_credentials["username"],
            password=self.db_credentials["password"],
            host=self.db_credentials["hostname"],
            port=self.db_credentials["port"],
        )
        cursor = connection.cursor()
        query = (
            "SELECT productId, entityId, download_url "
            "FROM scene_list "
            "WHERE cloudCover < %s AND path = %s AND row = %s "
            "ORDER BY acquisitionDate DESC "
            "LIMIT 1;"
        )
        data = (self.cloud_coverage, self.area.path, self.area.row)
        cursor.execute(query, data)
        try:
            product_id, entity_id, download_url = cursor.fetchone()
            log.info(
                "Found {} product with {} scene id, available for download "
                "through {} ".format(product_id, entity_id, download_url)
            )
        except TypeError:
            log.error(
                "Could not find any product for the {} area".format(self.area))
        else:
            return (product_id, entity_id, download_url)


class Landsat8DownloadOperator(BaseOperator):
    """Download a single Landsat8 file."""

    @apply_defaults
    def __init__(self, download_dir, get_inputs_from, url_fragment,
                 download_timeout=timedelta(hours=1), *args, **kwargs):
        super(Landsat8DownloadOperator, self).__init__(
            execution_timeout=download_timeout, *args, **kwargs)
        self.download_dir = download_dir
        self.get_inputs_from = get_inputs_from
        self.url_fragment = url_fragment

    def execute(self, context):
        task_inputs = context["task_instance"].xcom_pull(self.get_inputs_from)
        product_id, entity_id, download_url = task_inputs
        target_dir = os.path.join(self.download_dir, entity_id)
        try:
            os.makedirs(target_dir)
        except OSError as exc:
            if exc.errno == 17:  # directory already exists
                pass
        url = download_url.replace(
            "index.html", "{}_{}".format(product_id, self.url_fragment))
        target_path = os.path.join(
            target_dir,
            "{}_{}".format(product_id, self.url_fragment)
        )
        try:
            urllib.urlretrieve(url, target_path)
        except Exception:
            log.exception(
                msg="Error downloading {}".format(self.url_fragment))
            raise
        else:
            return target_path


class SearchDownloadDaraaPlugin(AirflowPlugin):
    name = "search_download_daraa_plugin"
    operators = [
        Landsat8SearchOperator,
        Landsat8DownloadOperator
    ]

from datetime import timedelta
import gzip
import logging
import os
import pprint

from airflow.operators import BaseOperator
from airflow.operators import BashOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import psycopg2
import requests

logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)


def download_file(url, destination_directory):
    response = requests.get(url, stream=True)
    try:
        os.makedirs(destination_directory)
    except OSError as exc:
        if exc.errno == 17:
            pass  # directory already exists
        else:
            raise
    full_path = os.path.join(destination_directory, url.rpartition("/")[-1])
    with open(full_path, "wb") as fh:
        for chunk in response.iter_content(chunk_size=1024):
            fh.write(chunk)
    return full_path


class DownloadSceneList(BaseOperator):

    @apply_defaults
    def __init__(self, download_url, download_dir, *args, **kwargs):
        super(DownloadSceneList, self).__init__(*args, **kwargs)
        self.download_url = download_url
        self.download_dir = download_dir

    def execute(self, context):
        logger.info("Downloading {!r}...".format(self.download_url))
        download_file(self.download_url, self.download_dir)
        return True


class ExtractSceneList(BaseOperator):

    @apply_defaults
    def __init__(self, path_to_extract, *args, **kwargs):
        super(ExtractSceneList, self).__init__(*args, **kwargs)
        self.path_to_extract = path_to_extract

    def execute(self, context):
        target_path = "{}.csv".format(
            os.path.splitext(self.path_to_extract)[0])
        logger.info("Extracting {!r} to {!r}...".format(
            self.path_to_extract, target_path))
        with gzip.open(self.path_to_extract, 'rb') as zipped_fh, \
                open(target_path, "wb") as extracted_fh:
            extracted_fh.write(zipped_fh.read())
        return True


class UpdateSceneList(BaseOperator):

    @apply_defaults
    def __init__(self, scene_list_path, pg_dbname, pg_hostname, pg_port,
                 pg_username,
                 pg_password,*args, **kwargs):
        self.scene_list_path = scene_list_path
        self.pg_dbname = pg_dbname
        self.pg_hostname = pg_hostname
        self.pg_port = pg_port
        self.pg_username = pg_username
        self.pg_password = pg_password
        super(UpdateSceneList, self).__init__(*args, **kwargs)

    def execute(self, context):
        logger.info("Connecting to '{}/{}'...".format(
            self.pg_hostname, self.pg_dbname))
        db_connection = psycopg2.connect(
            "dbname='{}' user='{}' host='{}' password='{}'".format(
                self.pg_dbname, self.pg_username, self.pg_hostname,
                self.pg_password
            )
        )
        logger.info("Deleting previous data from db...")
        with db_connection as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM scene_list;")
        logger.info("Loading data from {!r} into db...".format(
            self.scene_list_path))
        with db_connection as conn, open(self.scene_list_path) as fh:
            fh.readline()
            with conn.cursor() as cursor:
                cursor.copy_from(fh, "scene_list", sep=",")
        return True


class LANDSAT8DBPlugin(AirflowPlugin):
    name = "landsat8db_plugin"
    operators = [
        DownloadSceneList,
        ExtractSceneList,
        UpdateSceneList
    ]

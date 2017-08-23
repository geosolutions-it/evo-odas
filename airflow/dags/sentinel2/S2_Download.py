import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import DHUSSearchOperator, DHUSDownloadOperator, Sentinel2ThumbnailOperator, Sentinel2MetadataOperator, Sentinel2ProductZipOperator
from sentinel1.secrets import dhus_credentials
from sentinel1.config import sentinel2_config


log = logging.getLogger(__name__)

# Settings
default_args = {
    ##################################################
    # General configuration
    #
    'start_date': datetime.today() - timedelta(days=1),
    'owner': 'airflow',
    'depends_on_past': False,
    'provide_context': True,
    'email': ['airflow@evoodas.dlr.de'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'max_threads': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    #
}

# DAG definition
dag = DAG('S2_Download', description='DAG for searching, filtering and downloading Sentinel-2 data from DHUS server',
          default_args = default_args,
          dagrun_timeout = timedelta(hours=10),
          schedule_interval = '0 * * * *',
          catchup = False)

# Sentinel2- Search Task Operator
search_task = DHUSSearchOperator(task_id = 'dhus_search_task',
                                 dhus_url = 'https://scihub.copernicus.eu/dhus',
                                 dhus_user = dhus_credentials['username'],
                                 dhus_pass = dhus_credentials['password'],
                                 geojson_bbox = sentinel2_config['geojson_bbox'],
                                 startdate = sentinel2_config['startdate'],
                                 enddate = sentinel2_config['enddate'],
                                 platformname = sentinel2_config['platformname'],
                                 filename = sentinel2_config['filename'],
                                 dag = dag)

# Sentinel-2 Download Task Operator
download_task = DHUSDownloadOperator(task_id = 'dhus_download_task',
                                     dhus_url = 'https://scihub.copernicus.eu/dhus',
                                     dhus_user = dhus_credentials['username'],
                                     dhus_pass = dhus_credentials['password'],
                                     download_max = sentinel2_config['download_max'],
                                     download_dir = sentinel2_config['download_dir'],
                                     dag = dag)

# Sentinel-2 Create thumbnail Operator
thumbnail_task = Sentinel2ThumbnailOperator(task_id = 'dhus_thumbnail_task',
                                            thumb_size_x = '64',
                                            thumb_size_y = '64',
                                            dag=dag)
# Sentinel-2 Metadata Operator
metadata_task = Sentinel2MetadataOperator(task_id = 'dhus_metadata_task',
                                          dag = dag)

# Sentinel-2 Product.zip Operator
product_zip_task = Sentinel2ProductZipOperator(task_id = 'dhus_product_zip_task',
                                               target_dir = "/var/data/download/Sentinel-2",
                                               generated_files = ['product/product.json','product/granules.json','product/thumbnail.jpeg'],
                                               placeholders = ["/home/moataz/geo-solutions-work/evo-odas/airflow/plugins/templates/metadata.xml",
                                               "/home/moataz/geo-solutions-work/evo-odas/airflow/plugins/templates/owsLinks.json",
                                               "/home/moataz/geo-solutions-work/evo-odas/airflow/plugins/templates/product_description.html"],
                                               dag = dag)

search_task >> download_task >> thumbnail_task >> metadata_task >> product_zip_task

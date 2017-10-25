from datetime import datetime

import os

from airflow.models import DAG
from airflow.operators import DummyOperator
from airflow.operators import PythonOperator
from airflow.operators import GDALAddoOperator
from airflow.operators import GDALTranslateOperator
from airflow.operators import GDALInfoOperator
from airflow.operators import Landsat8DownloadOperator
from airflow.operators import Landsat8MTLReaderOperator
from airflow.operators import Landsat8ProductDescriptionOperator
from airflow.operators import Landsat8ProductZipFileOperator
from airflow.operators import Landsat8SearchOperator
from airflow.operators import Landsat8ThumbnailOperator
from airflow.operators import RSYNCOperator
from geoserver_plugin import publish_product
from landsat8_metadata_plugin import create_original_package

import config as CFG
import config.landsat8 as LANDSAT8


# These ought to be moved to a more central place where other settings might
# be stored
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(__file__)
        )
    )
)

TEMPLATES_PATH = os.path.join(PROJECT_ROOT, "metadata-ingestion", "templates")


def generate_dag(area, download_dir, default_args):
    """Generate Landsat8 ingestion DAGs.

    Parameters
    ----------
    area: Landsat8Area
        Configuration parameters for the Landsat8 area to be downloaded
    default_args: dict
        Default arguments for all tasks in the DAG.

    """

    dag = DAG(
        LANDSAT8.id + "_{}".format(area.name),
        description="DAG for downloading, processing and ingesting {} AOI in Landsat8 data "
                    "from scene_list".format(area.name),
        default_args=default_args,
        dagrun_timeout=LANDSAT8.dagrun_timeout,
        schedule_interval=LANDSAT8.dag_schedule_interval,
        catchup=LANDSAT8.catchup,
        params={
            "area": area,
        }
    )
    search_task = Landsat8SearchOperator(
        task_id='search_{}'.format(area.name),
        area=area,
        cloud_coverage=LANDSAT8.cloud_coverage,
        db_credentials= CFG.landsat8_postgresql_credentials,
        dag=dag
    )
    generate_html_description = Landsat8ProductDescriptionOperator(
        task_id='generate_html_description',
        description_template=os.path.join(
            TEMPLATES_PATH, "product_abstract.html"),
        download_dir=download_dir,
        dag=dag
    )
    download_thumbnail = Landsat8DownloadOperator(
        task_id="download_thumbnail",
        download_dir=download_dir,
        get_inputs_from=search_task.task_id,
        url_fragment="thumb_small.jpg",
        dag=dag
    )
    generate_thumbnail = Landsat8ThumbnailOperator(
        task_id='generate_thumbnail',
        get_inputs_from=download_thumbnail.task_id,
        thumb_size_x="64",
        thumb_size_y="64",
        dag=dag
    )
    download_metadata = Landsat8DownloadOperator(
        task_id="download_metadata",
        download_dir=download_dir,
        get_inputs_from=search_task.task_id,
        url_fragment="MTL.txt",
        dag=dag
    )

    join_task = DummyOperator(
        task_id='landsat8_join',
        dag=dag
    )

    download_tasks = []
    translate_tasks = []
    addo_tasks = []
    upload_tasks = []
    gdalinfo_tasks = []

    for band in area.bands:
        download_band = Landsat8DownloadOperator(
            task_id="download_band{}".format(band),
            download_dir=download_dir,
            get_inputs_from=search_task.task_id,
            url_fragment="B{}.TIF".format(band),
            dag=dag
        )
        download_tasks.append(download_band)

        translate = GDALTranslateOperator(
            task_id="translate_band{}".format(band),
            get_inputs_from=download_band.task_id,
            dag=dag
        )
        translate_tasks.append(translate)

        addo = GDALAddoOperator(
            task_id="add_overviews_band{}".format(band),
            get_inputs_from=translate.task_id,
            resampling_method="average",
            max_overview_level=128,
            compress_overview="PACKBITS",
            dag=dag
        )
        addo_tasks.append(addo)

        gdalinfo = GDALInfoOperator(
            task_id='landsat8_gdalinfo_band_{}'.format(band),
            get_inputs_from=addo.task_id,
            dag=dag
        )
        gdalinfo_tasks.append(gdalinfo)

        upload = RSYNCOperator(
            task_id="upload_band{}".format(band),
            host=CFG.rsync_hostname,
            remote_usr=CFG.rsync_username,
            ssh_key_file=CFG.rsync_ssh_key,
            remote_dir=LANDSAT8.repository_dir,
            get_inputs_from=addo.task_id,
            dag=dag)
        upload_tasks.append(upload)

        download_band.set_upstream(search_task)
        translate.set_upstream(download_band)
        addo.set_upstream(translate)
        gdalinfo.set_upstream(addo)
        upload.set_upstream(addo)
        join_task.set_upstream(upload)
        join_task.set_upstream(gdalinfo)

    download_task_ids = ( task.task_id for task in download_tasks )
    create_original_package_task = PythonOperator(task_id="create_original_package",
                                  python_callable=create_original_package,
                                  op_kwargs={
                                      'get_inputs_from': {
                                          "search_task_id"  : search_task.task_id,
                                          "download_task_ids" : download_task_ids,
                                      }
                                      ,
                                      'out_dir' : LANDSAT8.process_dir
                                  },
                                  dag=dag)

    upload_original_package_task = RSYNCOperator(
        task_id="upload_original_package",
        host=CFG.rsync_hostname,
        remote_usr=CFG.rsync_username,
        ssh_key_file=CFG.rsync_ssh_key,
        remote_dir=LANDSAT8.original_package_upload_dir,
        get_inputs_from=create_original_package_task.task_id,
        dag=dag)

    # we only neeed gdalinfo output on one of the granules
    gdalinfo_task = gdalinfo_tasks[0]
    gdalinfo_task_id = gdalinfo_task.task_id

    upload_task_ids = (task.task_id for task in upload_tasks)
    generate_metadata = Landsat8MTLReaderOperator(
        task_id='generate_metadata',
        original_package_download_base_url = LANDSAT8.original_package_download_base_url,
        gs_workspace = LANDSAT8.geoserver_workspace,
        gs_wms_layer = LANDSAT8.geoserver_layer,
        gs_wms_width = LANDSAT8.geoserver_oseo_wms_width,
        gs_wms_height = LANDSAT8.geoserver_oseo_wms_height,
        gs_wms_format = LANDSAT8.geoserver_oseo_wms_format,
        gs_wms_version = LANDSAT8.geoserver_oseo_wms_version,
        gs_wfs_featuretype = LANDSAT8.geoserver_featuretype,
        gs_wfs_format = LANDSAT8.geoserver_oseo_wfs_format,
        gs_wfs_version=LANDSAT8.geoserver_oseo_wfs_version,
        gs_wcs_layer = LANDSAT8.geoserver_layer,
        gs_wcs_scale_i = LANDSAT8.geoserver_oseo_wcs_scale_i,
        gs_wcs_scale_j = LANDSAT8.geoserver_oseo_wcs_scale_j,
        gs_wcs_format = LANDSAT8.geoserver_oseo_wcs_format,
        gs_wcs_version = LANDSAT8.geoserver_oseo_wcs_version,
        gs_wcs_coverage_id = LANDSAT8.geoserver_layer,
        get_inputs_from={
            "search_task_id"  : search_task.task_id,
            "metadata_task_id": download_metadata.task_id,
            "upload_task_ids" : upload_task_ids,
            "gdalinfo_task_id": gdalinfo_task_id,
            "upload_original_package_task_id": upload_original_package_task.task_id,
        },
        metadata_xml_path=os.path.join(TEMPLATES_PATH, "metadata.xml"),
        dag=dag
    )

    product_zip_task = Landsat8ProductZipFileOperator(
        task_id='landsat8_product_zip',
        get_inputs_from=[
            generate_html_description.task_id,
            generate_metadata.task_id,
            generate_thumbnail.task_id
        ],
        output_dir=LANDSAT8.process_dir,
        dag=dag
    )

    # curl -vvv -u evoadmin:\! -XPOST -H "Content-type: application/zip" --data-binary @/var/data/Sentinel-2/S2_MSI_L1C/download/S2A_MSIL1C_20170909T093031_N0205_R136_T36VUQ_20170909T093032/product.zip "http://ows-oda.eoc.dlr.de/geoserver/rest/oseo/collections/SENTINEL2/products"
    publish_task = PythonOperator(task_id="publish_product_task",
                                  python_callable=publish_product,
                                  op_kwargs={
                                      'geoserver_username': CFG.geoserver_username,
                                      'geoserver_password': CFG.geoserver_password,
                                      'geoserver_rest_endpoint': '{}/oseo/collections/{}/products'.format(
                                          CFG.geoserver_rest_url, LANDSAT8.geoserver_oseo_collection),
                                      'get_inputs_from': product_zip_task.task_id,
                                  },
                                  dag=dag)

    download_thumbnail.set_upstream(search_task)
    download_metadata.set_upstream(search_task)
    for tid in download_tasks:
        create_original_package_task.set_upstream(tid)
    upload_original_package_task.set_upstream(create_original_package_task)
    generate_metadata.set_upstream(join_task)
    generate_metadata.set_upstream(download_metadata)
    generate_metadata.set_upstream(upload_original_package_task)
    generate_thumbnail.set_upstream(download_thumbnail)
    generate_html_description.set_upstream(search_task)
    product_zip_task.set_upstream(generate_html_description)
    product_zip_task.set_upstream(generate_metadata)
    product_zip_task.set_upstream(generate_thumbnail)
    publish_task.set_upstream(upload_original_package_task)
    publish_task.set_upstream(product_zip_task)

    return dag


for area in LANDSAT8.AREAS:
    dag = generate_dag(area, download_dir=LANDSAT8.download_dir, default_args={
        'start_date': datetime(2017, 1, 1),
        'owner': 'airflow',
        'depends_on_past': False,
        'provide_context': True,
        'email': ['xyz@xyz.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'max_threads': 1,
    })
    globals()[dag.dag_id] = dag

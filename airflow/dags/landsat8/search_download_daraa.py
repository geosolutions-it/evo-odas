from collections import namedtuple
from datetime import datetime
from datetime import timedelta
import os

from airflow.models import DAG
from airflow.operators import GDALAddoOperator
from airflow.operators import GDALTranslateOperator
from airflow.operators import Landsat8DownloadOperator
from airflow.operators import Landsat8MTLReaderOperator
from airflow.operators import Landsat8ProductDescriptionOperator
from airflow.operators import Landsat8ProductZipFileOperator
from airflow.operators import Landsat8SearchOperator
from airflow.operators import Landsat8ThumbnailOperator

from landsat8.secrets import postgresql_credentials

# These ought to be moved to a more central place where other settings might
# be stored
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(__file__)
        )
    )
)
DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), "download")
TEMPLATES_PATH = os.path.join(PROJECT_ROOT, "metadata-ingestion", "templates")

Landsat8Area = namedtuple("Landsat8Area", [
    "name",
    "path",
    "row",
    "bands"
])


AREAS = [
    Landsat8Area(name="daraa", path=174, row=37, bands=[1, 2, 3]),
    # These are just some dummy areas in order to test generation of
    # multiple DAGs
    Landsat8Area(name="neighbour", path=175, row=37, bands=[1, 2, 3, 7]),
    Landsat8Area(name="other", path=176, row=37, bands=range(1, 12)),
]


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
       "Search_{}_Landsat8".format(area.name),
        description="DAG for searching and ingesting {} AOI in Landsat8 data "
                    "from scene_list".format(area.name),
        default_args=default_args,
        dagrun_timeout=timedelta(hours=1),
        schedule_interval=timedelta(days=1),
        catchup=False,
        params={
            "area": area,
        }
    )
    search_task = Landsat8SearchOperator(
        task_id='search_{}'.format(area.name),
        area=area,
        cloud_coverage=90.9,
        db_credentials=postgresql_credentials,
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
    generate_metadata = Landsat8MTLReaderOperator(
        task_id='generate_metadata',
        get_inputs_from=download_metadata.task_id,
        loc_base_dir='/efs/geoserver_data/coverages/landsat8/{}'.format(
            area.name),
        metadata_xml_path=os.path.join(TEMPLATES_PATH, "metadata.xml"),
        dag=dag
    )

    product_zip_task = Landsat8ProductZipFileOperator(
        task_id='landsat8_product_zip',
        get_inputs_from=[
            generate_html_description.task_id,
            generate_metadata.task_id,
            generate_thumbnail.task_id,
        ],
        output_dir=download_dir,
        dag=dag
    )
    for band in area.bands:
        download_band = Landsat8DownloadOperator(
            task_id="download_band{}".format(band),
            download_dir=download_dir,
            get_inputs_from=search_task.task_id,
            url_fragment="B{}.TIF".format(band),
            dag=dag
        )
        translate = GDALTranslateOperator(
            task_id="translate_band{}".format(band),
            get_inputs_from=download_band.task_id,
            dag=dag
        )
        addo = GDALAddoOperator(
            task_id="add_overviews_band{}".format(band),
            get_inputs_from=translate.task_id,
            resampling_method="average",
            max_overview_level=128,
            compress_overview="PACKBITS",
            dag=dag
        )
        download_band.set_upstream(search_task)
        translate.set_upstream(download_band)
        addo.set_upstream(translate)


    download_thumbnail.set_upstream(search_task)
    download_metadata.set_upstream(search_task)
    generate_metadata.set_upstream(download_metadata)
    generate_thumbnail.set_upstream(download_thumbnail)
    generate_html_description.set_upstream(search_task)
    product_zip_task.set_upstream(generate_html_description)
    product_zip_task.set_upstream(generate_metadata)
    product_zip_task.set_upstream(generate_thumbnail)
    return dag


for area in AREAS:
    dag = generate_dag(area, download_dir=DOWNLOAD_DIR, default_args={
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

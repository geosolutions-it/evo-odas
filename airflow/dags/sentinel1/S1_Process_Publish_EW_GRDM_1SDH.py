from airflow import DAG
from datetime import datetime
from sentinel1.s1_grd_subdag_factory import gdal_processing_sub_dag
from airflow.operators import ZipInspector, MockDownload, SubDagOperator

INTERVAL = '0 11 * * *'
DATE = datetime(2017, 5, 4)
DAG_NAME = 'S1_Process_Publish_EW_GRDM_1SDH'
SUBDAG_NAME = 'sentinel1_gdal'

default_args = {
    'start_date': datetime(2017, 1, 1),
    'owner': 'airflow',
    'depends_on_past': False,
    'provide_context': True,
    'email': ['airflow@evoodas.dlr.de'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'max_threads': 1,
}

main_dag = DAG(DAG_NAME, description='Sentinel1 ingestion flow',
                          schedule_interval=INTERVAL,
                          start_date=DATE,
                          catchup=False)

#search_task = DHUSSearchOperator(task_id='dhus_search_task', dag=dag)

#download_task = DHUSDownloadOperator(task_id='dhus_download_task', dag=dag)

mock_download_task = MockDownload(
    downloaded_path='/var/data/download/Sentinel-1/GRD/S1B_EW_GRDM_1SDH_20170605T041508_20170605T041530_005910_00A5D8_6AE4.zip',
    task_id='download',
    dag=main_dag
)

zip_task = ZipInspector(
    extension_to_search='tiff',
    task_id='zip_inspector',
    dag=main_dag
)

sentinel1_gdal_task = SubDagOperator(
    subdag = gdal_processing_sub_dag(DAG_NAME, SUBDAG_NAME, DATE, INTERVAL),
    task_id=SUBDAG_NAME,
    dag=main_dag
)

#zip_task >> sentinel1_gdal_task >> add_granule_task
mock_download_task >> zip_task >> sentinel1_gdal_task

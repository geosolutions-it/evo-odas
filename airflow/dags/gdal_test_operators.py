from datetime import datetime
from airflow import DAG
from airflow.operators import GDALWarpOperator, GDALAddoOperator

dag = DAG('gdal_test_dag', description='Tests if the GDAL operators works as expected',
          schedule_interval='* * * * *',
          start_date=datetime(2017, 5, 4), catchup=False)

working_dir = '/home/fds/Desktop/'
out_image = 'out_img.tiff'

warp = GDALWarpOperator(target_srs='EPSG:4326', tile_size='512', input_img_abs_path='/home/fds/Desktop/test_airflow.tiff', working_dir=working_dir, output_img_filename=out_image, overwrite=True,
                                task_id='gdal_warp', dag=dag)

addo = GDALAddoOperator(img_abs_path=working_dir + out_image, resampling_method='average', max_overview_level='128',
                                task_id='gdal_addo', dag=dag)

warp >> addo

from airflow.models import DAG
from airflow.operators import BaseOperator, BashOperator, Landsat8SearchOperator, Landsat8DownloadOperator, GDALTranslateOperator, GDALAddoOperator, Landsat8MTLReaderOperator, Landsat8ThumbnailOperator, Landsat8ProductDescriptionOperator, Landsat8ProductZipFileOperator 
from landsat8.secrets import postgresql_credentials
import logging
from datetime import datetime
from datetime import timedelta


##################################################
# General and shared configuration between tasks
##################################################
daraa_default_args = {
    'start_date': datetime(2017, 1, 1),
    'owner': 'airflow',
    'depends_on_past': False,
    'provide_context': True,
    'email': ['xyz@xyz.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'max_threads': 1,
}
######################################################
# Task specific configuration
######################################################
daraa_search_args = {\
	#'acquisition_date': '2017-04-11 05:36:29.349932',
	'cloud_coverage': 90.9,
	'path': 174,
	'row' : 37,
	'pgdbname':postgresql_credentials['dbname'],
	'pghostname':postgresql_credentials['hostname'],
	'pgport':postgresql_credentials['port'],
	'pgusername':postgresql_credentials['username'],
	'pgpassword':postgresql_credentials['password'],
}


daraa_download_args = {\
	'download_dir': '/var/data/download/',
	'number_of_bands' : 1
}

daraa_translate_args = {\
    'working_dir' : '/var/data/download/',
    'blockx_size' : '512',
    'blocky_size' : '512',
    'tiled'       : 'YES',
    'b'           : '1',
    'ot'          : 'UInt16',
    'of'          : 'GTiff',
    'outsize'     : '',
    'scale'       : ''
}

daraa_addo_args = {\
	'resampling_method'     : 'average',
	'max_overview_level'    : 128,
	'compress_overview'     : 'PACKBITS',
	'photometric_overview'  : 'MINISBLACK',
	'interleave_overview'   : ''
}

product_thumbnail_args = {\
	'thumb_size_x': '64',
	'thumb_size_y': '64'
}
# DAG definition
daraa_dag = DAG('Search_daraa_Landsat8', 
	description='DAG for searching Daraa AOI in Landsat8 data from scenes_list',
	default_args=daraa_default_args,
	dagrun_timeout=timedelta(hours=1),
	schedule_interval=timedelta(days=1),
	catchup=False)

# Landsat Search Task Operator
search_daraa_task = Landsat8SearchOperator(\
		task_id = 'landsat8_search_daraa_task',
		cloud_coverage = daraa_search_args['cloud_coverage'], 
		path = daraa_search_args['path'], 
		row = daraa_search_args['row'], 
		pgdbname = daraa_search_args['pgdbname'], 
		pghostname = daraa_search_args['pghostname'], 
		pgport = daraa_search_args['pgport'], 
		pgusername = daraa_search_args['pgusername'], 
		pgpassword = daraa_search_args['pgpassword'], 
		dag = daraa_dag
)

# Landsat Download Task Operator
download_daraa_task = Landsat8DownloadOperator(\
		task_id= 'landsat8_download_daraa_task', 
		download_dir = daraa_download_args['download_dir'],
		number_of_bands = daraa_download_args['number_of_bands'], 
		dag = daraa_dag
)

# Landsat gdal_translate Task Operator
translate_daraa_task = GDALTranslateOperator(\
		task_id= 'landsat8_translate_daraa_task',
		working_dir = daraa_translate_args["working_dir"],
		blockx_size = daraa_translate_args['blockx_size'],
		blocky_size = daraa_translate_args['blocky_size'],
		tiled = daraa_translate_args['tiled'],
		b = daraa_translate_args['b'],
		ot = daraa_translate_args['ot'],
		of = daraa_translate_args['of'],
		dag = daraa_dag)

addo_daraa_task = GDALAddoOperator(\
		task_id = 'landsat8_addo_daraa_task',
		xk_pull_dag_id = 'Search_daraa_Landsat8',
		xk_pull_task_id = 'landsat8_translate_daraa_task',
        	xk_pull_key_srcfile = 'translated_scenes_dir',
		resampling_method =  daraa_addo_args['resampling_method'],
		max_overview_level = daraa_addo_args['max_overview_level'],
		compress_overview = daraa_addo_args['compress_overview'],
		photometric_overview = daraa_addo_args['photometric_overview'],
		interleave_overview = daraa_addo_args['interleave_overview'],
		dag = daraa_dag)

product_json_task = Landsat8MTLReaderOperator(\
		task_id = 'landsat8_product_json_task', loc_base_dir = "/efs/geoserver_data/coverages/landsat8/daraa", dag = daraa_dag)

product_thumbnail_task = Landsat8ThumbnailOperator(\
		task_id = 'landsat8_product_thumbnail_task',
		thumb_size_x = product_thumbnail_args['thumb_size_x'],
		thumb_size_y = product_thumbnail_args['thumb_size_y'],
		dag = daraa_dag)

product_description_task = Landsat8ProductDescriptionOperator(\
		task_id = 'landsat8_product_description_task',
		dag = daraa_dag)

product_zip_task = Landsat8ProductZipFileOperator(\
		task_id = 'landsat8_product_zip_task',
		dag = daraa_dag)


search_daraa_task >> download_daraa_task >> translate_daraa_task >> addo_daraa_task >> product_json_task >> product_thumbnail_task >> product_description_task >> product_zip_task

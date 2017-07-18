import logging
from airflow.operators import BashOperator
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from zipfile import ZipFile
import config.xcom_keys as xk

log = logging.getLogger(__name__)

class GDALWarpOperator(BaseOperator):

    @apply_defaults
    def __init__(self, target_srs, tile_size, working_dir, overwrite, index, *args, **kwargs):
        self.target_srs = target_srs
        self.tile_size = str(tile_size)
        self.working_dir = working_dir
        self.index = index
        self.overwrite = '-overwrite' if overwrite else ''
        log.info('--------------------GDAL_PLUGIN Warp ------------')
        super(GDALWarpOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('--------------------GDAL_PLUGIN Warp running------------')
        task_instance = context['task_instance']
        parent_dag_id = context['dag'].dag_id
        log.info("Parent DAG: %s", parent_dag_id)
        input_img_abs_path = task_instance.xcom_pull('zip_inspector', key=xk.IMAGE_ZIP_ABS_PATH_PREFIX_XCOM_KEY + str(self.index), dag_id=parent_dag_id)
        output_img_filename = 'image_' + str(self.index) + '.tiff'
        log.info("GDAL Warp Operator params list")
        log.info('Target SRS: %s', self.target_srs)
        log.info('Tile size: %s', self.tile_size)
        log.info('INPUT img abs path: %s', input_img_abs_path)
        log.info('Working dir: %s', self.working_dir)
        log.info('OUTPUT filename: %s', output_img_filename)
        gdalwarp_command = 'gdalwarp ' + self.overwrite + ' -t_srs ' + self.target_srs + ' -co TILED=YES -co BLOCKXSIZE=' + self.tile_size + ' -co BLOCKYSIZE=' + self.tile_size + ' ' + input_img_abs_path + ' ' + self.working_dir + "/" + output_img_filename
        log.info('The complete GDAL warp command is: %s', gdalwarp_command)
        task_instance.xcom_push(key=xk.WORKDIR_IMG_NAME_PREFIX_XCOM_KEY + str(self.index), value=self.working_dir + '/' + output_img_filename)
        bo = BashOperator(task_id="bash_operator_warp_" + str(self.index), bash_command=gdalwarp_command)
        bo.execute(context)


class GDALAddoOperator(BaseOperator):

    @apply_defaults
    def __init__(self, resampling_method, max_overview_level, index, compress_overview = None, photometric_overview = None, interleave_overview = None, *args, **kwargs):
        self.resampling_method = resampling_method
        self.max_overview_level = max_overview_level
        self.index = index
        self.compress_overview = ' --config COMPRESS_OVERVIEW ' + compress_overview +' ' if compress_overview else ' '
        self.photometric_overview = ' --config PHOTOMETRIC_OVERVIEW ' + photometric_overview +' ' if photometric_overview else ' '
        self.interleave_overview = ' --config INTERLEAVE_OVERVIEW ' + interleave_overview +' ' if interleave_overview else ' '
        level = 2
        levels = ''
        while(level <= int(self.max_overview_level)):
            level = level*2
            levels += str(level)
            levels += ' '
        self.levels = levels
        log.info('-------------------- GDAL_PLUGIN Addo ------------')
        super(GDALAddoOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        task_instance = context['task_instance']
        img_abs_path = task_instance.xcom_pull('gdal_warp_' + str(self.index), key=xk.WORKDIR_IMG_NAME_PREFIX_XCOM_KEY + str(self.index))
        log.info("GDAL Warp Addo params list")
        log.info('Resampling method: %s', self.resampling_method)
        log.info('Max overview level: %s', self.max_overview_level)
        log.info('COMPRESS_OVERVIEW: %s', self.compress_overview)
        log.info('PHOTOMETRIC_OVERVIEW: %s', self.photometric_overview)
        log.info('INTERLEAVE_OVERVIEW: %s', self.interleave_overview)
        gdaladdo_command = 'gdaladdo -r ' + self.resampling_method + ' ' + self.compress_overview + self.photometric_overview+ self.interleave_overview + img_abs_path + ' ' + self.levels
        log.info('The complete GDAL addo command is: %s', gdaladdo_command)
        bo = BashOperator(task_id='bash_operator_addo_', bash_command=gdaladdo_command)
        bo.execute(context)

class GDALTranslateOperator(BaseOperator):

    @apply_defaults
    def __init__(self, 
            tiled = None, 
            working_dir = None,
            blockx_size = None,
            blocky_size = None,
            compress = None,
            photometric = None,
            ot = None, of = None,
            b = None, mask = None,
            outsize = None,
            scale = None,
            *args, **kwargs):

        self.working_dir = working_dir
 
        self.tiled = ' -co "TILED=' + tiled +'" ' if tiled else ' '
        self.blockx_size = ' -co "BLOCKXSIZE=' + blockx_size + '" ' if blockx_size else ' '
        self.blocky_size = ' -co "BLOCKYSIZE=' + blocky_size + '" ' if blocky_size else ' '
        self.compress = ' -co "COMPRESS=' + compress + '"' if compress else ' '
        self.photometric = ' -co "PHOTOMETRIC=' + photometric + '" ' if photometric else ' '

        self.ot = ' -ot ' + str(ot) if ot else ''
        self.of = ' -of ' + str(of) if of else ''
        self.b = ' -b ' + str(b) if b else ''
        self.mask = '-mask ' + str(mask) if mask else ''
        self.outsize = '-outsize ' + str(outsize) if outsize else ''
        self.scale = ' -scale ' + str(scale) if scale else ''

        log.info('--------------------GDAL_PLUGIN Translate initiated------------')
        super(GDALTranslateOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('--------------------GDAL_PLUGIN Translate running------------')
        task_instance = context['task_instance']
        log.info("GDAL Translate Operator params list")
        log.info('Working dir: %s', self.working_dir)
        scene_fullpath = context['task_instance'].xcom_pull('download_task', key='scene_fullpath')
        output_img_filename = 'translated_' +str(scene_fullpath.rsplit('/', 1)[-1])+ '.TIF'
        gdaltranslate_command = 'gdal_translate ' + self.ot + self.of + self.b + self.mask + self.outsize + self.scale + self.tiled + self.blockx_size + self.blocky_size + self.compress + self.photometric + scene_fullpath + '  ' + self.working_dir + "/" + output_img_filename
        log.info('The complete GDAL translate command is: %s', gdaltranslate_command)
        task_instance.xcom_push(key='daraa_translated_image', value=str(self.working_dir) + "/" + str(output_img_filename))
        bo = BashOperator(task_id="bash_operator_translate_daraa", bash_command=gdaltranslate_command)
        bo.execute(context)
        return True

class GDALPlugin(AirflowPlugin):
    name = "GDAL_plugin"
    operators = [GDALWarpOperator, GDALAddoOperator, GDALTranslateOperator]

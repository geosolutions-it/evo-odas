import logging

from airflow.operators import BashOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)

class GDALWarpOperator(BashOperator):

    @apply_defaults
    def __init__(self, target_srs, tile_size, input_img_abs_path, working_dir, output_img_filename, overwrite, *args, **kwargs):
        self.target_srs = target_srs
        self.tile_size = tile_size
        self.input_img_abs_path = input_img_abs_path
        self.working_dir = working_dir
        self.output_img_filename = output_img_filename
        self.overwrite = '-overwrite' if overwrite else ''
        self.gdalwarp_command= 'gdalwarp ' + self.overwrite + ' -t_srs ' + self.target_srs + ' -co TILED=YES -co BLOCKXSIZE=' + self.tile_size + ' -co BLOCKYSIZE=' + self.tile_size + ' ' + self.input_img_abs_path + ' ' + self.working_dir + self.output_img_filename
        super(GDALWarpOperator, self).__init__(bash_command=self.gdalwarp_command, *args, **kwargs)

    def execute(self, context):
        log.info("GDAL Warp Operator params list")
        log.info('Target SRS: %s', self.target_srs)
        log.info('Tile size: %s', self.tile_size)
        log.info('INPUT img abs path: %s', self.input_img_abs_path)
        log.info('Working dir: %s', self.working_dir)
        log.info('OUTPUT filename: %s', self.output_img_filename)
        log.info('The full command is: %s', self.gdalwarp_command)
        BashOperator.execute(self, context)

class GDALAddoOperator(BashOperator):

    @apply_defaults
    def __init__(self, img_abs_path, resampling_method, max_overview_level, *args, **kwargs):
        self.resampling_method = resampling_method
        self.max_overview_level = max_overview_level
        self.img_abs_path = img_abs_path
        level = 2
        levels = ''
        while(level <= int(self.max_overview_level)):
            level = level*2
            levels += str(level)
            levels += ' '
        self.gdalwarp_command= 'gdaladdo -r ' + self.resampling_method + ' ' + self.img_abs_path + ' ' + levels
        super(GDALAddoOperator, self).__init__(bash_command=self.gdalwarp_command, *args, **kwargs)

    def execute(self, context):
        log.info("GDAL Warp Addo params list")
        log.info('Resampling method: %s', self.resampling_method)
        log.info('Max overview level: %s', self.max_overview_level)
        log.info('The full command is: %s', self.gdalwarp_command)
        BashOperator.execute(self, context)

class GDALPlugin(AirflowPlugin):
    name = "GDAL_plugin"
    operators = [GDALWarpOperator, GDALAddoOperator]

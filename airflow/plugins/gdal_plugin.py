from itertools import count
import logging
import os
import pprint
from subprocess import check_output

from airflow.operators import BashOperator
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import XCOM_RETURN_KEY

log = logging.getLogger(__name__)


def get_overview_levels(max_level):
    levels = []
    current = 2
    while current <= max_level:
        levels.append(current)
        current *= 2
    return levels


def get_gdaladdo_command(source, overview_levels, resampling_method,
                         compress_overview=None):
    compress_token = (
        "--config COMPRESS_OVERVIEW {}".format(compress_overview) if
        compress_overview is not None else ""
    )
    return "gdaladdo {compress} -r {method} {src} {levels}".format(
        method=resampling_method,
        compress=compress_token,
        src=source,
        levels=" ".join(str(level) for level in overview_levels),
    )


def get_gdal_translate_command(source, destination, output_type,
                               creation_options):
    return (
        "gdal_translate -ot {output_type} {creation_opts} "
        "{src} {dst}".format(
            output_type=output_type,
            creation_opts=_get_gdal_creation_options(**creation_options),
            src=source,
            dst=destination
        )
    )


class GDALWarpOperator(BaseOperator):

    @apply_defaults
    def __init__(self, target_srs, tile_size, overwrite, dstdir, get_inputs_from=None,
                 *args, **kwargs):
        self.target_srs = target_srs
        self.tile_size = str(tile_size)
        self.overwrite = overwrite
        self.dstdir = dstdir
        self.get_inputs_from = get_inputs_from

        super(GDALWarpOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('--------------------GDAL_PLUGIN Warp running------------')
        task_instance = context['task_instance']
        log.info("""
            target_srs: {}
            tile_size: {}
            overwrite: {}
            dstdir: {}
            get_inputs_from: {}
            """.format(
            self.target_srs,
            self.tile_size,
            self.overwrite,
            self.dstdir,
            self.get_inputs_from,
            )
        )

        dstdir = self.dstdir

        input_paths= task_instance.xcom_pull(self.get_inputs_from, key=XCOM_RETURN_KEY)
        if input_paths is None:
            log.info('Nothing to do')
            return None

        output_paths=[]
        for srcfile in input_paths:
            log.info('srcfile: %s', srcfile)
            srcfilename = os.path.basename(srcfile)
            dstfile = os.path.join(dstdir, srcfilename)
            log.info('dstfile: %s', dstfile)

            # build gdalwarp command
            self.overwrite = '-overwrite' if self.overwrite else ''
            gdalwarp_command = (
                'gdalwarp ' + self.overwrite + ' -t_srs ' + self.target_srs +
                ' -co TILED=YES -co BLOCKXSIZE=' + self.tile_size +
                ' -co BLOCKYSIZE=' + self.tile_size + ' ' + srcfile + ' ' +
                dstfile
            )
            log.info('The complete GDAL warp command is: %s', gdalwarp_command)
            bo = BashOperator(task_id="bash_operator_warp", bash_command=gdalwarp_command)
            bo.execute(context)
            output_paths.append(dstfile)

        return output_paths


class GDALAddoOperator(BaseOperator):

    @apply_defaults
    def __init__(self, get_inputs_from, resampling_method,
                 max_overview_level, compress_overview=None,
                 *args, **kwargs):
        super(GDALAddoOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from
        self.resampling_method = resampling_method
        self.max_overview_level = int(max_overview_level)
        self.compress_overview = compress_overview

    def execute(self, context):
        input_paths = context["task_instance"].xcom_pull(self.get_inputs_from, key=XCOM_RETURN_KEY)
        input_path = input_paths[0]

        levels = get_overview_levels(self.max_overview_level)
        log.info("Generating overviews for {!r}...".format(input_path))
        command = get_gdaladdo_command(
            input_path, overview_levels=levels,
            resampling_method=self.resampling_method,
            compress_overview=self.compress_overview
        )
        output_path= input_path
        bo = BashOperator(
            task_id='bash_operator_addo_{}'.format(
                os.path.basename(input_path)),
            bash_command=command
        )
        bo.execute(context)
        return output_path


class GDALTranslateOperator(BaseOperator):

    @apply_defaults
    def __init__(self, get_inputs_from, output_type="UInt16",
                 creation_options=None, *args, **kwargs):
        super(GDALTranslateOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from
        self.output_type = str(output_type)
        self.creation_options = dict(
            creation_options) if creation_options is not None else {
            "tiled": True,
            "blockxsize": 512,
            "blockysize": 512,
        }

    def execute(self, context):
        downloaded_path = context["task_instance"].xcom_pull(
            self.get_inputs_from)
        working_dir = os.path.join(os.path.dirname(downloaded_path),
                                   "__translated")
        try:
            os.makedirs(working_dir)
        except OSError as exc:
            if exc.errno == 17:
                pass  # directory already exists
            else:
                raise

        output_img_filename = 'translated_{}'.format(
            os.path.basename(downloaded_path))
        output_path = os.path.join(working_dir, output_img_filename)
        command = get_gdal_translate_command(
            source=downloaded_path, destination=output_path,
            output_type=self.output_type,
            creation_options=self.creation_options
        )
        log.info("The complete GDAL translate command is: {}".format(command))
        b_o = BashOperator(
            task_id="bash_operator_translate",
            bash_command=command
        )
        b_o.execute(context)

        output_paths = [output_path]
        return output_paths

class GDALInfoOperator(BaseOperator):

    @apply_defaults
    def __init__(self, get_inputs_from, *args, **kwargs):
        super(GDALInfoOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from

    def execute(self, context):
        input_paths = []
        for get_input in self.get_inputs_from:
            input_paths.append(context["task_instance"].xcom_pull(get_input))
        gdalinfo_outputs = {}
        for input_path in input_paths:
            command = ["gdalinfo"]
            log.info("Running GDALInfo on {}...".format(input_path))
            command.append(input_path)
            gdalinfo_output = check_output(command)
            log.info("{}".format(gdalinfo_output))
            gdalinfo_outputs[input_path] = gdalinfo_output
        return gdalinfo_outputs


class GDALPlugin(AirflowPlugin):
    name = "GDAL_plugin"
    operators = [
        GDALWarpOperator,
        GDALAddoOperator,
        GDALTranslateOperator,
        GDALInfoOperator
    ]


def _get_gdal_creation_options(**creation_options):
    result = ""
    for name, value in creation_options.items():
        opt = '-co "{}={}"'.format(name.upper(), value)
        " ".join((result, opt))
    return result


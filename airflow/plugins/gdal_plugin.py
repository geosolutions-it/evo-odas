from itertools import count
import logging
import os

from airflow.operators import BashOperator
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

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
    def __init__(self, target_srs, tile_size, overwrite, srcfile=None,
                 dstdir=None, xk_pull_dag_id=None, xk_pull_task_id=None,
                 xk_pull_key_srcfile=None, xk_pull_key_dstdir=None,
                 xk_push_key=None, *args, **kwargs):
        self.target_srs = target_srs
        self.tile_size = str(tile_size)
        self.overwrite = overwrite
        self.srcfile = srcfile
        self.dstdir = dstdir
        self.xk_pull_dag_id = xk_pull_dag_id
        self.xk_pull_task_id = xk_pull_task_id
        self.xk_pull_key_srcfile = xk_pull_key_srcfile
        self.xk_pull_key_dstdir = xk_pull_key_dstdir
        self.xk_push_key = xk_push_key
        super(GDALWarpOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('--------------------GDAL_PLUGIN Warp running------------')
        task_instance = context['task_instance']
        log.info("""
            target_srs: {}
            tile_size: {}
            overwrite: {}
            srcfile: {}
            dstdir: {}
            xk_pull_task_id: {}
            xk_pull_key_srcfile: {}
            xk_pull_key_dstdir: {}
            xk_push_key: {}
            xk_pull_dag_id: {}
            """.format(
            self.target_srs,
            self.tile_size,
            self.overwrite,
            self.srcfile,
            self.dstdir,
            self.xk_pull_task_id,
            self.xk_pull_key_srcfile,
            self.xk_pull_key_dstdir,
            self.xk_push_key,
            self.xk_pull_dag_id
            )
        )
        # init XCom parameters
        if self.xk_pull_dag_id is None:
            self.xk_pull_dag_id = context['dag'].dag_id
        # check input file path passed otherwise look for it in XCom
        if self.srcfile is not None:
            srcfile = self.srcfile
        else:
            if self.xk_pull_key_srcfile is None:
                self.xk_pull_key_srcfile = 'srcdir'
            log.debug('Fetching srcfile from XCom')
            srcfile = task_instance.xcom_pull(
                task_ids=self.xk_pull_task_id,
                key=self.xk_pull_key_srcfile,
                dag_id=self.xk_pull_dag_id
            )
            if srcfile is None:
                log.warn('No srcfile fetched from XCom. Nothing to do')
                return False
        assert srcfile is not None
        log.info('srcfile: %s', srcfile)

        # extract filename and directory path
        srcfilename = os.path.basename(srcfile)
        srcdir = os.path.dirname(srcfile)

        # check output file path passed otherwise try to find it in XCom
        if self.dstdir is not None:
            dstdir = self.dstdir
        else:
            if self.xk_pull_key_dstdir is None:
                self.xk_pull_key_dstdir = 'dstdir'
            log.debug('Fetching dstdir from XCom')
            dstdir = task_instance.xcom_pull(
                task_ids=self.xk_pull_task_id,
                key=self.xk_pull_key_dstdir,
                dag_id=context['dag'].dag_id
            )
            log.info('No dstdir fetched from XCom')
        # not found in XCom? use source directory
        if dstdir is None:
            log.info("using srcdir as dstdir")
            dstdir = srcdir
        log.info('dstdir: %s', dstdir)
        dstfile = os.path.join(dstdir, srcfilename)
        assert dstfile is not None
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

        # push output path to XCom
        if self.xk_push_key is None:
            self.xk_push_key = 'dstfile'
        task_instance.xcom_push(key='dstfile', value=dstfile)

        # run the command
        bo = BashOperator(
            task_id="bash_operator_warp", bash_command=gdalwarp_command)
        bo.execute(context)


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
        input_path = context["task_instance"].xcom_pull(self.get_inputs_from)
        levels = get_overview_levels(self.max_overview_level)
        log.info("Generating overviews for {!r}...".format(input_path))
        command = get_gdaladdo_command(
            input_path, overview_levels=levels,
            resampling_method=self.resampling_method,
            compress_overview=self.compress_overview
        )
        bo = BashOperator(
            task_id='bash_operator_addo_{}'.format(
                os.path.basename(input_path)),
            bash_command=command
        )
        bo.execute(context)


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
        return output_path


class GDALPlugin(AirflowPlugin):
    name = "GDAL_plugin"
    operators = [
        GDALWarpOperator,
        GDALAddoOperator,
        GDALTranslateOperator
    ]


def _get_gdal_creation_options(**creation_options):
    result = ""
    for name, value in creation_options.items():
        opt = '-co "{}={}"'.format(name.upper(), value)
        " ".join((result, opt))
    return result


"""
/*********************************************************************************/
 *  The MIT License (MIT)                                                         *
 *                                                                                *
 *  Copyright (c) 2014 EOX IT Services GmbH                                       *
 *                                                                                *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy  *
 *  of this software and associated documentation files (the "Software"), to deal *
 *  in the Software without restriction, including without limitation the rights  *
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell     *
 *  copies of the Software, and to permit persons to whom the Software is         *
 *  furnished to do so, subject to the following conditions:                      *
 *                                                                                *
 *  The above copyright notice and this permission notice shall be included in    *
 *  all copies or substantial portions of the Software.                           *
 *                                                                                *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR    *
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,      *
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE   *
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER        *
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, *
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE *
 *  SOFTWARE.                                                                     *
 *                                                                                *
 *********************************************************************************/
"""

from itertools import count
import logging
import os
import six
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

def _get_gdal_creation_options(**creation_options):
    result = ""
    for name, value in creation_options.items():
        opt = '-co "{}={}"'.format(name.upper(), value)
        " ".join((result, opt))
    return result


class GDALWarpOperator(BaseOperator):
    """ Execute gdalwarp with given options on list of files fetched from XCom. Returns output files paths to XCom.

    Args:
        target_srs (str): parameter for gdalwarp
        tile_size (str): parameter for gdalwarp
        overwrite (str): parameter for gdalwarp
        dstdir (str): output files directory
        get_inputs_from (str): task_id used to fetch input files list from XCom

    Returns:
        list: list of output files path
    """

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
            log.info('Nothing to process')
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
    """ Execute gdaladdo with given options on list of files fetched from XCom. Returns output files paths to XCom.

    Args:
        resampling_method (str): parameter for gdaladdo
        max_overview_level (str): parameter for gdaladdo
        compress_overview (str): parameter for gdaladdo
        get_inputs_from (str): task_id used to fetch input files list from XCom

    Returns:
        list: list containing output files path
    """

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
        if input_paths is None:
            log.info("Nothing to process")
            return None

        output_paths = []
        for input_path in input_paths:
            levels = get_overview_levels(self.max_overview_level)
            log.info("Generating overviews for {!r}...".format(input_path))
            command = get_gdaladdo_command(
                input_path, overview_levels=levels,
                resampling_method=self.resampling_method,
                compress_overview=self.compress_overview
            )
            output_path = input_path
            output_paths.append(output_path)
            bo = BashOperator(
                task_id='bash_operator_addo_{}'.format(
                    os.path.basename(input_path)),
                bash_command=command
            )
            bo.execute(context)

        return output_paths


class GDALTranslateOperator(BaseOperator):
    """ Execute gdaltranslate with given options on file fetched from XCom. Returns output file paths to XCom.

    Args:
        creation_options (str): parameter for gdaltranslate
        output_type (str): parameter for gdaltranslate
        get_inputs_from (str): task_id used to fetch input files list from XCom

    Returns:
        list: list containing output files path
    """

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
        input_paths = context["task_instance"].xcom_pull(self.get_inputs_from, key=XCOM_RETURN_KEY)
        if input_paths is None:
            log.info("Nothing to process")
            return None

        # If message from XCom is a string with single file path, turn it into a string
        if isinstance(input_paths, six.string_types):
            input_paths = [ input_paths ]

        working_dir = os.path.join(os.path.dirname(input_paths[0]),
                                   "__translated")
        try:
            os.makedirs(working_dir)
        except OSError as exc:
            if exc.errno == 17:
                pass  # directory already exists
            else:
                raise

        output_paths = []
        for input_path in input_paths:
            output_img_filename = 'translated_{}'.format(
                os.path.basename(input_path))
            output_path = os.path.join(working_dir, output_img_filename)
            output_paths.append(output_path)
            command = get_gdal_translate_command(
                source=input_path, destination=output_path,
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
    """ Execute gdalinfo with given options on list of files fetched from XCom. Returns output files paths to XCom.

    Args:
        get_inputs_from (str): task_id used to fetch input files list from XCom

    Returns:
        dict: dictionary mapping input files to the matching gdalinfo output
    """

    @apply_defaults
    def __init__(self, get_inputs_from, *args, **kwargs):
        super(GDALInfoOperator, self).__init__(*args, **kwargs)
        self.get_inputs_from = get_inputs_from

    def execute(self, context):
        input_paths = context["task_instance"].xcom_pull(self.get_inputs_from, key=XCOM_RETURN_KEY)

        if input_paths is None:
            log.info("Nothing to process")
            return None

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

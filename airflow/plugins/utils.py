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

import os
import logging
import fnmatch
import pprint
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.models import XCOM_RETURN_KEY

from jinja2 import Environment, FileSystemLoader, Template

import config as CFG
log = logging.getLogger(__name__)

class TemplatesResolver:

    def __init__(self):
        dirname = os.path.dirname(os.path.abspath(__file__))
        log = logging.getLogger(__name__)
        log.info(pprint.pformat(CFG.templates_base_dir))
        self.j2_env = Environment(loader=FileSystemLoader(CFG.templates_base_dir))

    def generate_product_abstract(self, product_abstract_metadata_dict):
        return self.j2_env.get_template('product_abstract.html').render(product_abstract_metadata_dict)

    def generate_sentinel1_product_metadata(self, metadata_dict):
        return self.j2_env.get_template('sentinel1_metadata.xml').render(metadata_dict)

    def generate_sentinel2_product_metadata(self, metadata_dict):
        return self.j2_env.get_template('sentinel2_metadata.xml').render(metadata_dict)

    def generate_sentinel2_product_metadata(self, metadata_dict):
        return self.j2_env.get_template('sentinel2_metadata.xml').render(metadata_dict)


class MoveFilesOperator(BaseOperator):
    @apply_defaults
    def __init__(self, src_dir, dst_dir, filter, *args, **kwargs):
        super(MoveFilesOperator, self).__init__(*args, **kwargs)
        self.src_dir = src_dir
        self.dst_dir = dst_dir
        self.filter = filter
        log.info('--------------------MoveFilesOperator------------')

    def execute(self, context):
        log.info('\nsrc_dir={}\ndst_dir={}\nfilter={}'.format(self.src_dir, self.dst_dir, self.filter))
        filenames = fnmatch.filter(os.listdir(self.src_dir), self.filter)
        for filename in filenames:
            filepath = os.path.join(self.src_dir, filename)
            if not os.path.exists(self.dst_dir):
                log.info("Creating directory {}".format(self.dst_dir))
                os.makedirs(self.dst_dir)
            dst_filename = os.path.join(self.dst_dir, os.path.basename(filename))
            log.info("Moving {} to {}".format(filepath, dst_filename))
            #os.rename(filepath, dst_filename)

class UtilsPlugin(AirflowPlugin):
    name = "Utils_plugin"
    operators = [MoveFilesOperator]
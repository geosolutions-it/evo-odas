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
from datetime import datetime, timedelta

#
# Airflow root directory
#
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.dirname(__file__)
    )
)

#
# Paths
#
base_dir = os.getenv('BASE_DIR','/var/data/')
regions_base_dir = os.path.join(base_dir, 'regions')
repository_base_dir = os.getenv('REPOSITORY_DIR',os.path.join(base_dir, 'repository'))
templates_base_dir = os.getenv('TEMPLATES_DIR', os.path.join(PROJECT_ROOT,'plugins','templates' ))

#
# Connections
#
dhus_url = 'https://scihub.copernicus.eu/dhus'
dhus_username = ''
dhus_password = ''
dhus_filter_max = 2
dhus_download_max = dhus_filter_max

geoserver_rest_url = 'http://localhost:8080/geoserver/rest'
geoserver_username = 'admin'
geoserver_password = ''

#eoxserver_rest_url = 'http://localhost:8080/eoxserver/product/'
eoxserver_rest_url = None
eoxserver_username = ''
eoxserver_password = ''

postgresql_dbname = 'oseo'
postgresql_hostname = 'localhost'
postgresql_port = '5432'
postgresql_username = 'postgres'
postgresql_password = ''

rsync_hostname = 'localhost'
rsync_username = os.getenv('USER','airflow')
rsync_ssh_key = os.path.join(os.getenv('HOME','/usr/local/airflow'),'.ssh','id_rsa')
rsync_remote_dir = '/tmp'

#
# Dates
#
# yesterday at beginning of day
yesterday_start = datetime.now() - timedelta(days=1)
yesterday_start = yesterday_start.replace(hour=0, minute=0, second=0, microsecond=0)
yesterday_start = yesterday_start.isoformat() + 'Z'
# yesterday at end of day
yesterday_end = datetime.now() - timedelta(days=1)
yesterday_end = yesterday_end.replace(hour=23, minute=59, second=59, microsecond=999999)
yesterday_end = yesterday_end.isoformat() + 'Z'
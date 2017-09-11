import requests
import logging
from pprint import pprint, pformat

log = logging.getLogger(__name__)

def publish_product(geoserver_username, geoserver_password, geoserver_rest_endpoint, *args, **kwargs):
    # Pull Zip path from XCom
    log.info("publish_product task")
    log.info("""
        geoserver_username: {}
        geoserver_password: {}
        geoserver_rest_endpoint: {}
        """.format(
        geoserver_username,
        geoserver_password,
        geoserver_rest_endpoint
        )
    )
    task_instance = kwargs['ti']
    zip_file = task_instance.xcom_pull('product_zip_task', key='product_zip_path')
    log.info("zip_file_path: {}".format(zip_file))

    # POST product.zip
    d = open(zip_file, 'rb').read()
    a = requests.auth.HTTPBasicAuth(geoserver_username, geoserver_password)
    h = {'Content-type': 'application/zip'}

    r = requests.post(geoserver_rest_endpoint,
        auth=a,
        data=d,
        headers=h)

    log.info('response\n{}'.format(pformat(r.text)))
    if not r.status_code == requests.codes.ok:
        r.raise_for_status()
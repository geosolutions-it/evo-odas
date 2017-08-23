#!/usr/bin/env python
from jinja2 import Environment, FileSystemLoader, Template
import os
import logging
import pprint

#from sentinel1.templates import ogc_links_templates as lt

class TemplatesResolver:

    def __init__(self):
        dirname = os.path.dirname(os.path.abspath(__file__))
        pardirname = os.path.abspath(os.path.join(dirname, os.pardir))
        template_dir = os.path.join(pardirname, 'templates')
        log = logging.getLogger(__name__)
        log.info(pprint.pformat(template_dir))
        self.j2_env = Environment(loader=FileSystemLoader(template_dir))

    def generate_product_abstract(self, product_abstract_metadata_dict):
        return self.j2_env.get_template('product_abstract.html').render(product_abstract_metadata_dict)

    def generate_sentinel1_product_metadata(self, metadata_dict):
        return self.j2_env.get_template('sentinel1_metadata.xml').render(metadata_dict)

    def generate_sentinel2_product_metadata(self, metadata_dict):
        return self.j2_env.get_template('sentinel2_metadata.xml').render(metadata_dict)

    def generate_sentinel2_product_metadata(self, metadata_dict):
        return self.j2_env.get_template('sentinel2_metadata.xml').render(metadata_dict)
    """
    def generate_ogc_links(self, href_params_dict):
        ogc_links = []
        for link in lt.links:
            link_out = list(link)
            if lt.protocols.count(link[0]) > 0:
                link_out[5] = Template(link[5]).render(href_params_dict)
                ogc_links.append(link_out)
        return ogc_links
    """
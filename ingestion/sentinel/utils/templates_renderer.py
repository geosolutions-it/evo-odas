#!/usr/bin/env python
import re
from jinja2 import Environment, FileSystemLoader
import os

class TemplatesResolver:

    def __init__(self):
        TEMPLATE_DIR = os.path.dirname(os.path.abspath(__file__)).join(('templates',))
        self.j2_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))

    def generate_product_abstract(self, product_abstract_metadata_dict):
        return self.j2_env.get_template('product_abstract.html').render(product_abstract_metadata_dict)

    def generate_sentinel1_product_metadata(self, metadata_dict):
        return self.j2_env.get_template('sentinel1_metadata.xml').render(metadata_dict)

    def generate_sentinel2_product_metadata(self, metadata_dict):
        return self.j2_env.get_template('sentinel2_metadata.xml').render(metadata_dict)

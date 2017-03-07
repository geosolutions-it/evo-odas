#!/usr/bin/env python
import re
from jinja2 import Environment, FileSystemLoader
import configs.webserver_paths as wp
from pgmagick import Image
import os

def generate_product_abstract(product_abstract_metadata_dict):
    new_file_name = wp.preview_folder + os.path.basename(product_abstract_metadata_dict["PVI_IMG_PATH"])
    new_file_name = re.sub(".jp2",".jpg", new_file_name)
    img = Image(product_abstract_metadata_dict["PVI_IMG_PATH"])
    img.write(wp.document_root + new_file_name)
    product_abstract_metadata_dict["PVI_IMG_PATH"] = new_file_name
    TEMPLATE_DIR = os.path.dirname(os.path.abspath(__file__)).join(('templates',))
    j2_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    return j2_env.get_template('product_abstract.html').render(product_abstract_metadata_dict)

def generate_product_metadata(metadata_dict):
    TEMPLATE_DIR = os.path.dirname(os.path.abspath(__file__)).join(('templates',))
    j2_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    return j2_env.get_template('sentinel2_metadata.xml').render(metadata_dict)

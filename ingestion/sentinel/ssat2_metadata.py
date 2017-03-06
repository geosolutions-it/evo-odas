#!/usr/bin/env python
import s2reader
import sys
import os
import re
import pg_simple
from jinja2 import Environment, FileSystemLoader
from shutil import copy
import configs.metadata_db_connection_params as db_config
import utils.metadata_utils as mu
import utils.dictionary_utils as du
import configs.webserver_paths as wp
from pgmagick import Image

def collect_sentinel2_metadata(safe_pkg, granule):
    return ({
                # USED IN METADATA TEMPLATE and as SEARCH PARAMETERS
                'timeStart':safe_pkg.product_start_time,
                'timeEnd':safe_pkg.product_stop_time,
                'eoParentIdentifier':"S2_MSI_L1C",# from Torsten velocity template see related mail in ML
                'eoAcquisitionType':"NOMINAL",# from Torsten velocity template see related mail in ML
                'eoOrbitNumber':safe_pkg.sensing_orbit_number,
                'eoOrbitDirection':safe_pkg.sensing_orbit_direction,
                'optCloudCover':granule.cloud_percent,
                'eoCreationDate':safe_pkg.generation_time,
                'eoArchivingCenter':"DPA",# from Torsten velocity template see related mail in ML
                'eoProcessingMode':"DATA_DRIVEN",# from Torsten velocity template see related mail in ML
                'footprint':str(granule.footprint),
                'name':granule.granule_identifier
            },
            {
                # USED IN METADATA TEMPLATE ONLY
                'eoProcessingLevel':safe_pkg.processing_level,
                'eoSensorType':"OPTICAL",
                'eoOrbitType':"LEO",
                'eoProductType':safe_pkg.product_type,
                'eoInstrument':safe_pkg.product_type[2:5],
                'eoPlatform':safe_pkg.spacecraft_name[0:10],
                'eoPlatformSerialIdentifier':safe_pkg.spacecraft_name[10:11]
            },
            {
                #TO BE USED IN THE PRODUCT ABSTRACT TEMPLATE
                'PVI_IMG_PATH':granule.pvi_path,
                'timeStart':safe_pkg.product_start_time,
                'timeEnd':safe_pkg.product_stop_time,
                'ATOM_SEARCH_URL':"TBD",
                'SRU_SEARCH_URL':"TBD",
                'METADATA_SEARCH_URL':"TBD",
            })

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

def check_granule_identifier(granule_identifier):
    pg_simple.config_pool(max_conn=db_config.max_conn,
                            expiration=db_config.expiration,
                            host=db_config.host,
                            port=db_config.port,
                            database=db_config.database,
                            user=db_config.user,
                            password=db_config.password)
    with pg_simple.PgSimple() as db:
        collection = db.fetchone('metadata.product',
                                    fields=['"name"'],
                                    where=('"name" = %s', [granule_identifier]))
        if collection == None:
            return False
        else:
            return True

def persist_product_search_params(dict_to_persist):
    with pg_simple.PgSimple() as db:
        collection = db.fetchone('metadata.collection',
                                    fields=['"eoIdentifier"'],
                                    where=('"eoIdentifier" = %s', ["SENTINEL2"]))
        if collection is None:
            raise LookupError("ERROR: No related collection found!")
        dict_to_persist['"eoParentIdentifier"'] = collection.eoIdentifier
        row = db.insert('metadata.product', data=dict_to_persist, returning='id')
        db.commit()
    return row.id

def persist_product_metadata(xmlDoc, id):
    with pg_simple.PgSimple() as db:
        db.insert('metadata.product_metadata', data={'metadata':xmlDoc, 'id':id})
        db.commit()

def main(args):
    if len(args) > 1:
       raise Error("too many parameters!")
    print "arg is: '" + args[0] + "'"

    with s2reader.open(args[0]) as safe_pkg:
        #mu.print_metadata(safe_pkg)
        for granule in safe_pkg.granules:
            if(check_granule_identifier(granule.granule_identifier)):
                print "ERROR: Granule '" + granule.granule_identifier + "' already exist, skipping it..."
                continue
            (search_params, other_metadata, product_abstract_metadata) = collect_sentinel2_metadata(safe_pkg, granule)
            htmlAbstract = generate_product_abstract(product_abstract_metadata)
            xml_doc = generate_product_metadata(du.join(search_params, other_metadata))
            try:
                search_params['htmlDescription'] = htmlAbstract
                id = persist_product_search_params(du.wrap_keys_among_brackets(search_params))
            except  LookupError:
                print "ERROR: No related collection found!"
                break
            persist_product_metadata(xml_doc, id)

if __name__ == "__main__":
    main(sys.argv[1:])

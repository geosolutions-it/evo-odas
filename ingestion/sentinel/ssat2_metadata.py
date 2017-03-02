#!/usr/bin/env python
import s2reader
import sys
import os
import pg_simple
from jinja2 import Environment, FileSystemLoader
import configs.metadata_db_connection_params as db_config
import utils.metadata_utils as mu
import utils.dictionary_utils as du

def collect_sentinel2_metadata(safe_pkg, granule):
    return ({
                # TO BE USED IN METADATA TEMPLATE and as SEARCH PARAMETERS
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
                'footprint':str(granule.footprint)
            },
            {
                # TO BE USED IN METADATA TEMPLATE
                'eoProcessingLevel':safe_pkg.processing_level,
                'eoSensorType':"OPTICAL",
                'eoOrbitType':"LEO",
                'eoProductType':safe_pkg.product_type,
                'eoInstrument':safe_pkg.product_type[2:5],
                'eoPlatform':safe_pkg.spacecraft_name[0:10],
                'eoPlatformSerialIdentifier':safe_pkg.spacecraft_name[10:11]
            })

def generate_product_metadata(metadata_dict):
    TEMPLATE_DIR = os.path.dirname(os.path.abspath(__file__)).join(('templates',))
    j2_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    return j2_env.get_template('sentinel2_metadata.xml').render(metadata_dict)

def persist_product_search_params(dict_to_persist):
    pg_simple.config_pool(max_conn=db_config.max_conn,
                            expiration=db_config.expiration,
                            host=db_config.host,
                            port=db_config.port,
                            database=db_config.database,
                            user=db_config.user,
                            password=db_config.password)
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
            (search_params, other_metadata) = collect_sentinel2_metadata(safe_pkg, granule)
            xmlDoc = generate_product_metadata(du.join(search_params, other_metadata))
            try:
                id = persist_product_search_params(du.wrap_keys_among_brackets(search_params))
            except  LookupError:
                print "ERROR: No related collection found!"
                break
            persist_product_metadata(xmlDoc, id)

if __name__ == "__main__":
    main(sys.argv[1:])

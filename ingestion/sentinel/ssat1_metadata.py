#!/usr/bin/env python
import sys
import utils.metadata as mu
import utils.dictionary as du
from utils.templates_renderer import TemplatesResolver
from utils.metadata_storage import PostgresStorage


def collect_sentinel1_metadata(metadata):
    return ({
                # USED IN METADATA TEMPLATE and as SEARCH PARAMETERS
                'timeStart':metadata['ACQUISITION_START_TIME'],
                'timeEnd':metadata['ACQUISITION_STOP_TIME'],
                'eoOrbitNumber':metadata['ORBIT_NUMBER'],
                'eoOrbitDirection':metadata['ORBIT_DIRECTION'],
                'eoArchivingCenter':"DPA",# from Torsten velocity template see related mail in ML
                'eoProcessingMode':"DATA_DRIVEN",# from Torsten velocity template see related mail in ML
                'optCloudCover':0,
                'eoSwathIdentifier':metadata['SWATH'],
                #'footprint':str(granule.footprint),
                'name':metadata['NAME'],
            },
            {
                # USED IN METADATA TEMPLATE ONLY
                'eoProcessingLevel':"L1",
                'eoSensorType':"RADAR",
                'eoProductType':metadata['PRODUCT_TYPE'],
                'eoInstrument':metadata['SENSOR_IDENTIFIER'],
                'eoPlatform':metadata['SATELLITE_IDENTIFIER'],
                'eoPlatformSerialIdentifier':metadata['MISSION_ID'],
            },
            {
                #TO BE USED IN THE PRODUCT ABSTRACT TEMPLATE
                'timeStart':metadata['ACQUISITION_START_TIME'],
                'timeEnd':metadata['ACQUISITION_STOP_TIME'],
            })

def main(args):
    if len(args) > 1:
       raise Error("too many parameters!")
    print "+++++ Sentinel1 User Product filename: '" + args[0] + "'"

    s1reader = S1GDALReader(args[0])
    storage = PostgresStorage()
    tr = TemplatesResolver()

    print "--- Processing granule: '" + granule_identifier + "'"
    if(storage.check_granule_identifier(granule_identifier)):
        print "WARNING: Granule '" + granule_identifier + "' already exist, skipping it..."
        return
    (search_params, other_metadata, product_abstract_metadata) = collect_sentinel1_metadata(s1reader.get_metadata())
    htmlAbstract = tr.generate_product_abstract(product_abstract_metadata)
    xml_doc = tr.generate_sentinel1_product_metadata(du.join(search_params, other_metadata))
    try:
        search_params['htmlDescription'] = htmlAbstract
        id = storage.persist_product_search_params(du.wrap_keys_among_brackets(search_params), "SENTINEL1")
        storage.persist_thumb(mu.create_thumb(s1reader.get_preview_image()), id)
    except  LookupError:
        print "ERROR: No related collection found!"
        return
    storage.persist_product_metadata(xml_doc, id)

if __name__ == "__main__":
    main(sys.argv[1:])

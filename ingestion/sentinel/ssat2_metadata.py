#!/usr/bin/env python
import s2reader
import sys
import utils.metadata as mu
import utils.dictionary as du
from utils.templates_renderer import TemplatesResolver
from utils.metadata_storage import PostgresStorage


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
                'timeStart':safe_pkg.product_start_time,
                'timeEnd':safe_pkg.product_stop_time,
                'PRODUCT_NAME':granule.granule_identifier,
                'ATOM_SEARCH_URL':"TBD",
                'SRU_SEARCH_URL':"TBD",
                'METADATA_SEARCH_URL':"TBD",
            })

def main(args):
    if len(args) > 1:
       raise Error("too many parameters!")
    print "+++++ Sentinel2 User Product filename: '" + args[0] + "'"

    storage = PostgresStorage()
    tr = TemplatesResolver()

    with s2reader.open(args[0]) as safe_pkg:
        #mu.print_metadata(safe_pkg)
        for granule in safe_pkg.granules:
            print "--- Processing granule: '" + granule.granule_identifier + "'"
            if(storage.check_granule_identifier(granule.granule_identifier)):
                print "WARNING: Granule '" + granule.granule_identifier + "' already exist, skipping it..."
                continue
            (search_params, other_metadata, product_abstract_metadata) = collect_sentinel2_metadata(safe_pkg, granule)
            htmlAbstract = tr.generate_product_abstract(product_abstract_metadata)
            xml_doc = tr.generate_product_metadata(du.join(search_params, other_metadata))
            try:
                search_params['htmlDescription'] = htmlAbstract
                id = storage.persist_product_search_params(du.wrap_keys_among_brackets(search_params))
                storage.persist_thumb(mu.create_thumb(granule.pvi_path), id)
            except  LookupError:
                print "ERROR: No related collection found!"
                break
            storage.persist_product_metadata(xml_doc, id)

if __name__ == "__main__":
    main(sys.argv[1:])

#!/usr/bin/env python
import s2reader
import sys
import utils.metadata as mu
import utils.dictionary as du
from utils.templates_renderer import TemplatesResolver
from utils.metadata_storage import PostgresStorage
import utils.pg_mapping as pgmap

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
            (search_params, other_metadata, product_abstract_metadata) = pgmap.collect_sentinel2_metadata(safe_pkg, granule)
            htmlAbstract = tr.generate_product_abstract(product_abstract_metadata)
            xml_doc = tr.generate_sentinel2_product_metadata(du.join(search_params, other_metadata))
            try:
                search_params['htmlDescription'] = htmlAbstract
                id = storage.persist_product_search_params(du.wrap_keys_among_brackets(search_params), "SENTINEL2")
                storage.persist_thumb(mu.create_thumb(granule.pvi_path), id)
            except  LookupError:
                print "ERROR: No related collection found!"
                break
            storage.persist_product_metadata(xml_doc, id)
            ogc_bbox = storage.get_product_OGC_BBOX(granule.granule_identifier)
            storage.persist_ogc_links(pgmap.create_ogc_links_dict(tr.generate_ogc_links(pgmap.ogc_links_href_dict(ogc_bbox, id))), id)

if __name__ == "__main__":
    main(sys.argv[1:])

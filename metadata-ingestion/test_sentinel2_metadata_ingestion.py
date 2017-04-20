#!/usr/bin/env python
import sys
import s2reader
import utils.metadata as mu
import os
from utils.templates_renderer import TemplatesResolver
import ssat2_metadata as s2
from utils.metadata_storage import PostgresStorage
import utils.pg_mapping as pgmap

def test_metadata_read(pkg_path):
    with s2reader.open(pkg_path) as safe_pkg:
        mu.print_metadata(safe_pkg)

def test_product_abstract_generation(pkg_path):
    tr = TemplatesResolver()
    with s2reader.open(pkg_path) as safe_pkg:
        for granule in safe_pkg.granules:
            (search_params, other_metadata, product_abstract_metadata) = s2.collect_sentinel2_metadata(safe_pkg, granule)
            print tr.generate_product_abstract(product_abstract_metadata)

def test_ogc_links():
    tr = TemplatesResolver()
    print tr.generate_ogc_links({})

def update_OGC_links():
    ps = PostgresStorage()
    tr = TemplatesResolver()
    for el in ps.get_products_id():
        ogc_bbox = ps.get_product_OGC_BBOX(el[1])
        list = pgmap.create_ogc_links_dict(tr.generate_ogc_links(pgmap.ogc_links_href_dict(ogc_bbox, el[0])))
        ps.persist_ogc_links(list, el[0])

def test_ingestion(pkg_path):
    ps = PostgresStorage()
    ps.persist_collection({
        '"eoIdentifier"':"SENTINEL2"
    })
    s2.main([pkg_path])

def test_bbox():
    ps = PostgresStorage()
    print ps.get_product_OGC_BBOX("S2A_OPER_MSI_L1C_TL_SGS__20160929T154211_A006640_T32TPP_N02.04")

def update_original_package_location(folder):
    ps = PostgresStorage()
    safe_pkgs = os.listdir(folder)
    print safe_pkgs
    print "searching SAFE packages from the: '" + folder + "' directory"
    for f in safe_pkgs:
        print "------> " + f
        if f.endswith(".SAFE"):
            with s2reader.open(folder+f) as safe_pkg:
                for g in safe_pkg.granules:
                    ps.update_original_package_location(f, g.granule_identifier);


def main(args):
    pkg_path = "test_data/S2A_OPER_PRD_MSIL1C_PDMC_20160929T185902_R065_V20160929T102022_20160929T102344.SAFE"
    #test_product_abstract_generation(pkg_path)
    #test_metadata_read(pkg_path)
    #test_ingestion(pkg_path)
    update_original_package_location("/home/fds/Desktop/test_folder/")
    #test_bbox()
    #test_ogc_links()
    #update_OGC_links()

if __name__ == "__main__":
    main(sys.argv[1:])

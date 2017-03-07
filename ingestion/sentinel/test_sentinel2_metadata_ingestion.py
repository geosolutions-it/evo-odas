#!/usr/bin/env python
import sys
import s2reader
import utils.metadata_utils as mu
import ssat2_metadata as s2

def test_metadata_read(pkg_path):
    with s2reader.open(pkg_path) as safe_pkg:
        mu.print_metadata(safe_pkg)

def test_product_abstract_generation(pkg_path):
    with s2reader.open(pkg_path) as safe_pkg:
        for granule in safe_pkg.granules:
            (search_params, other_metadata, product_abstract_metadata) = s2.collect_sentinel2_metadata(safe_pkg, granule)
            print s2.generate_product_abstract(product_abstract_metadata)

def main(args):
    pkg_path = "test_data/S2A_OPER_PRD_MSIL1C_PDMC_20160929T185902_R065_V20160929T102022_20160929T102344.SAFE"
    test_product_abstract_generation(pkg_path)
    test_metadata_read(pkg_path)

if __name__ == "__main__":
    main(sys.argv[1:])

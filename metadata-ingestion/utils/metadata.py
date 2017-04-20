#!/usr/bin/env python
import s2reader
import configs.workflows_settings as ws
from pgmagick import Blob, Image

def create_thumb(pvi_img_path):
    img = Image(Blob(open(pvi_img_path).read()))
    img.scale(ws.thumb_size_x + 'x' + ws.thumb_size_y)
    img.quality(ws.thumb_jpeg_compression_quality)
    img.write(ws.working_directory + "tmp_thumb_resized.jpg")
    blob = Blob(open(ws.working_directory + "tmp_thumb_resized.jpg").read())
    return blob.data

def print_metadata(safe_pkg):
    print "--------------------PRODUCT---------------------------------------------"
    print "safe_pkg: '" + str(safe_pkg) + "'"
    print "start_time: '" + str(safe_pkg.product_start_time) + "'"
    print "stop_time: '" + str(safe_pkg.product_stop_time) + "'"
    print "generation_time: '" + str(safe_pkg.generation_time) + "'"
    print "_product_metadata: '" + str(safe_pkg._product_metadata) + "'"
    print "_manifest_safe: '" + str(safe_pkg._manifest_safe) + "'"
    print "product_metadata_path: '" + str(safe_pkg.product_metadata_path) + "'"
    print "processing_level: '" + str(safe_pkg.processing_level) + "'"
    print "footprint_char_length: '" + str(len(str(safe_pkg.footprint))) + "'"
    print "----------------------------------------------------------------------------"
    for granule in safe_pkg.granules:
        print "--------------------GRANULE-----------------------------------------"
        print "granule.granule_path: '" + granule.granule_path
        print "granule.granule_identifier: '" + granule.granule_identifier
        print "footprint_char_length: '" + str(len(str(granule.footprint)))
        print "srid: '" +granule.srid
        print "metadata_path: '" + granule.metadata_path
        print "cloud_percent: '" + str(granule.cloud_percent)
        #print "cloudmask: '" + str(len(str(granule.cloudmask)))
        print "nodata_mask: '" + str(granule.nodata_mask)
        print "band_path: '" + str(granule.band_path)

        print "--------------------------------------------------------------------"

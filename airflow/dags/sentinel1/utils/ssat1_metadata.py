#!/usr/bin/env python
import sys
import os
import logging
import pprint
import json
from shutil import copyfile
from zipfile import ZipFile
from S1Reader import S1GDALReader
from templates_renderer import TemplatesResolver
import config as CFG
import config.s1_grd_1sdv as S1GRD1SDV

# OWS links settings for WMS
GS_WMS_WORKSPACE=S1GRD1SDV.geoserver_workspace
GS_WMS_LAYER=S1GRD1SDV.geoserver_layer
GS_WMS_VERSION="1.3.0"
GS_WMS_WIDTH=S1GRD1SDV.geoserver_oseo_wms_width
GS_WMS_HEIGHT=S1GRD1SDV.geoserver_oseo_wms_height
GS_WMS_FORMAT=S1GRD1SDV.geoserver_oseo_wms_format

# OWS links settings for WFS
GS_WFS_WORKSPACE=S1GRD1SDV.geoserver_workspace
GS_WFS_LAYER=S1GRD1SDV.geoserver_featuretype
GS_WFS_VERSION="2.0.0"
GS_WFS_FORMAT="application/json"

# OWS links settings for WCS
GS_WCS_WORKSPACE=S1GRD1SDV.geoserver_workspace
GS_WCS_LAYER=S1GRD1SDV.geoserver_coverage
GS_WCS_VERSION="2.0.1"
GS_WCS_SCALE_I='0.1'
GS_WCS_SCALE_J='0.1'
GS_WCS_FORMAT="geotiff"

WORKING_DIR=S1GRD1SDV.process_dir

log = logging.getLogger(__name__)

def collect_sentinel1_metadata(metadata):
    return ({   # USED IN METADATA TEMPLATE and as SEARCH PARAMETERS
                "type": "Feature",
                "geometry": {
                    "type": metadata['footprint']['type'],
                    "coordinates": metadata['footprint']['coordinates']
                },
                "properties": {
                    "eop:identifier": metadata['NAME'],
                    'timeStart': metadata['ACQUISITION_START_TIME'],
                    'timeEnd': metadata['ACQUISITION_STOP_TIME'],
                    "originalPackageLocation": None,
                    "thumbnailURL": None,
                    "quicklookURL": None,
                    "eop:parentIdentifier": "SENTINEL1",
                    "eop:productionStatus": None,
                    "eop:acquisitionType": None,
                    'eop:orbitNumber':metadata['ORBIT_NUMBER'],
                    'eop:orbitDirection':metadata['ORBIT_DIRECTION'],
                    "eop:track": None,
                    "eop:frame": None,
                    "eop:swathIdentifier": metadata['SWATH'],
                    "opt:cloudCover": None,
                    "opt:snowCover": None,
                    "eop:productQualityStatus": None,
                    "eop:productQualityDegradationStatus": None,
                    "eop:processorName": None,
                    "eop:processingCenter": metadata['FACILITY_IDENTIFIER'], #?
                    "eop:creationDate": None,
                    "eop:modificationDate": None,
                    "eop:processingDate": None,
                    "eop:sensorMode": metadata['BEAM_MODE'],
                    "eop:archivingCenter": None,
                    "eop:processingMode": None,
                    "eop:availabilityTime": None,
                    "eop:acquisitionStation": None,
                    "eop:acquisitionSubtype": None,
                    "eop:startTimeFromAscendingNode": None,
                    "eop:completionTimeFromAscendingNode": None,
                    "eop:illuminationAzimuthAngle": None,
                    "eop:illuminationZenithAngle": None,
                    "eop:illuminationElevationAngle": None,
                    "sar:polarisationMode": None,
                    "sar:polarisationChannels": None,
                    "sar:antennaLookDirection": None,
                    "sar:minimumIncidenceAngle": None,
                    "sar:maximumIncidenceAngle": None,
                    "sar:dopplerFrequency": None,
                    "sar:incidenceAngleVariation": None,
                    "eop:resolution": None,
                },
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


def create_procuct_zip(sentinel1_product_zip_path,
                       granules_dict,
                       safe_package_filename,
                       original_package_download_base_url,
                       owslinks_dict,
                       working_dir=WORKING_DIR
                       ):
    s1reader = S1GDALReader(sentinel1_product_zip_path)
    files = []

    s1metadata = s1reader.get_metadata()
    log.info(pprint.pformat(s1metadata, indent=4))

    s1metadata['footprint'] = s1reader.get_footprint()
    log.info(pprint.pformat(s1metadata['footprint'], indent=4))

    (search_params, other_metadata, product_abstract_metadata) = collect_sentinel1_metadata(s1metadata)

    # Add OriginalPackage Location
    search_params['properties']['originalPackageLocation'] = original_package_download_base_url + safe_package_filename

    log.info(pprint.pformat(search_params))
    log.info(pprint.pformat(other_metadata))
    log.info(pprint.pformat(product_abstract_metadata))

    if not os.path.exists(working_dir):
        os.makedirs(working_dir)

    # create description.html and dump it to file
    log.info("Creating description.html")
    tr = TemplatesResolver()
    try:
        htmlAbstract = tr.generate_product_abstract(product_abstract_metadata)
    except:
        log.error("could not render template for Sentinel1 HTML abstract")
    log.debug(pprint.pformat(htmlAbstract))
    search_params['htmlDescription'] = htmlAbstract

    path = os.path.join(working_dir, "description.html")
    with open(path, "w") as f:
        f.write(htmlAbstract)
    files.append(path)

    # create metadata.xml and dump it to file
    log.info("Creating metadata.xml")
    # xml_doc = tr.generate_sentinel1_product_metadata(du.join(search_params, other_metadata))
    try:
        metadata_xml = tr.generate_sentinel1_product_metadata(product_abstract_metadata)
    except:
        log.error("could not render template for Sentinel1 metadata.xml")
    log.debug(pprint.pformat(metadata_xml))

    path = os.path.join(working_dir, "metadata.xml")
    with open(path, "w") as f:
        f.write(metadata_xml)
    files.append(path)

    # create thumbnail
    # TODO: create proper thumbnail from quicklook. Also remove temp file
    log.info("Creating thumbnail")
    path = os.path.join(working_dir, "thumbnail.png")
    quicklook_path = s1reader.get_quicklook()
    log.info(pprint.pformat(quicklook_path))
    copyfile(quicklook_path, path)
    files.append(path)

    # create granules.json
    log.info("Creating granules.json")

    log.info( "granules_dict: \n{}".format(pprint.pformat(granules_dict)) )
    path = os.path.join(working_dir, 'granules.json')
    with open(path, 'w') as f:
        json.dump(granules_dict, f, indent=4)
    files.append(path)

    # Use the coordinates from the granules in the product.json as the s1reader seems to
    # swap the footprint coordinates, see https://github.com/geosolutions-it/evo-odas/issues/192
    granule_coords=granules_dict.get('features')[0].get('geometry').get('coordinates')
    search_params['geometry']['coordinates']=granule_coords
    
    # dump product.json to file
    log.info("Creating product.json")
    path = os.path.join(working_dir, 'product.json')
    with open(path, 'w') as f:
        json.dump(search_params, f, indent=4)
    files.append(path)

    # create owslinks.json and dump it to file
    log.info("Creating owslinks.json")
    path = os.path.join(working_dir, 'owslinks.json')
    with open(path, 'w') as f:
        json.dump(owslinks_dict, f, indent=4)
    files.append(path)

    # create product.zip
    log.info("Creating product zip with files: " + pprint.pformat(files))
    path = os.path.join(working_dir, 'product.zip')
    try:
        with ZipFile(path, 'w') as z:
            for f in files:
                z.write(f, os.path.basename(f))
    except:
        log.error("failed to create product.zip")

    # cleanup
    for file in files:
        os.remove(file)

    return path

def main(sentinel1_product_zip_path):
    log = logging.getLogger(__name__)
    granules_paths = []
    create_procuct_zip(sentinel1_product_zip_path, granules_paths)

if __name__ == "__main__":
    main(sys.argv[1])


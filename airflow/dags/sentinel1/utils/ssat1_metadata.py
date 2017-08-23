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


GS_WORKSPACE="test"
GS_LAYER="SENTINEL1_MOSAIC"
GS_WMS_WIDTH="768"
GS_WMS_HEIGHT="768"
GS_WMS_FORMAT="image/jpeg"
WORKING_DIR='/var/data/working'

try:
    from osgeo import gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules, install gdal with python bindings')

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

def collect_granules_metadata(granules_paths, granules_upload_dir):
    # create granules.json
    granules_dict = {
        "type": "FeatureCollection",
        "features": []
    }

    for granule in granules_paths:
        granule_name = os.path.basename(granule)
        location = os.path.join(granules_upload_dir, granule_name)
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": None
            },
            "properties": {
                "location": location
            }
        }
        datastore = gdal.Open(granule)
        metadata_dict = datastore.GetMetadata()
        log.info(pprint.pformat(metadata_dict))

        # list of list of lists
        coordinates = []
        bbox_polygon = get_bbox_from_granule(granule)
        bbox_str="{},{},{},{}".format(bbox_polygon[0][0],bbox_polygon[1][1],bbox_polygon[3][0],bbox_polygon[3][1])
        log.info("bbox_str: " + pprint.pformat(bbox_str))
        coordinates.append(bbox_polygon)
        feature['geometry']['coordinates'] = coordinates
        log.info("COORDINATES: " + pprint.pformat(coordinates))
        granules_dict['features'].append(feature)
        return (granules_dict, bbox_str)

def create_owslinks_dict(product_metadata, bbox):
    owslinks_dict = \
        {
        "links": [
            {
                "offering": "http://www.opengis.net/spec/owc/1.0/req/atom/wms",
                "method": "GET",
                "code": "GetCapabilities",
                "type": "application/xml",
                "href": "${BASE_URL}" + "/{}/{}/ows?service=wms&request={}&version={}&CQL_FILTER=eoParentIdentifier='{}'".format(
                    GS_WORKSPACE, GS_LAYER, "GetCapabilities", "1.3.0", product_metadata['NAME'])
            },
            {
                "offering": "http://www.opengis.net/spec/owc/1.0/req/atom/wms",
                "method": "GET",
                "code": "GetMap",
                "type": "image/jpeg",
                "href": "${BASE_URL}" + "/{}/{}/ows?service=wms&request={}&version={}&LAYERS={}&BBOX={}&WIDTH={}&HEIGHT={}&FORMAT={}&CQL_FILTER=eoIdentifier='{}'".format(
                    GS_WORKSPACE, GS_LAYER, "GetMap", "1.3.0", GS_LAYER, bbox, GS_WMS_WIDTH, GS_WMS_HEIGHT, GS_WMS_FORMAT, product_metadata['NAME'])
            }
        ]
    }
    return owslinks_dict

def get_bbox_from_granule(granule_path):
    datastore = gdal.Open(granule_path)
    ulx, xres, xskew, uly, yskew, yres = datastore.GetGeoTransform()
    lrx = ulx + (datastore.RasterXSize * xres)
    lry = uly + (datastore.RasterYSize * yres)
    llx, lly = (ulx, lry)
    urx, ury = (lrx, uly)
    return [ [ulx,uly], [llx,lly], [lrx,lry], [urx,ury], [ulx,uly] ]

def create_procuct_zip(sentinel1_product_zip_path, granules_paths, granules_upload_dir):
    working_dir = WORKING_DIR
    s1reader = S1GDALReader(sentinel1_product_zip_path)
    files = []

    s1metadata = s1reader.get_metadata()
    log.info(pprint.pformat(s1metadata, indent=4))

    s1metadata['footprint'] = s1reader.get_footprint()
    log.info(pprint.pformat(s1metadata['footprint'], indent=4))

    (search_params, other_metadata, product_abstract_metadata) = collect_sentinel1_metadata(s1metadata)
    log.info(pprint.pformat(search_params))
    log.info(pprint.pformat(other_metadata))
    log.info(pprint.pformat(product_abstract_metadata))

    # dump product.json to file
    path = os.path.join(working_dir, 'product.json')
    with open(path, 'w') as f:
        json.dump(search_params, f, indent=4)
    files.append(path)

    # create description.html and dump it to file
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
    path = os.path.join(working_dir, "thumbnail.png")
    quicklook_path = s1reader.get_quicklook()
    log.info(pprint.pformat(quicklook_path))
    copyfile(quicklook_path, path)
    files.append(path)

    # create granules.json
    granules_dict, bbox_str = collect_granules_metadata(granules_paths, granules_upload_dir)
    log.info(pprint.pformat(granules_dict))
    path = os.path.join(working_dir, 'granules.json')
    with open(path, 'w') as f:
        json.dump(granules_dict, f, indent=4)
    files.append(path)

    # create owslinks.json and dump it to file
    owslinks_dict = create_owslinks_dict(s1metadata, bbox_str)
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

    return s1metadata

def main(sentinel1_product_zip_path):
    log = logging.getLogger(__name__)
    granules_paths = []
    create_procuct_zip(sentinel1_product_zip_path, granules_paths)

if __name__ == "__main__":
    main(sys.argv[1])

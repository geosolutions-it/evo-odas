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
GS_WFS_LAYER=S1GRD1SDV.geoserver_layer
GS_WFS_VERSION="2.0.0"
GS_WFS_FORMAT="application/json"

# OWS links settings for WCS
GS_WCS_WORKSPACE=S1GRD1SDV.geoserver_workspace
GS_WCS_LAYER=S1GRD1SDV.geoserver_coverage
GS_WCS_VERSION="2.0.1"
GS_WCS_SCALE_I='0.1'
GS_WCS_SCALE_J='0.1'
GS_WCS_FORMAT="geotiff"

# Band's numbers for S1
band_number = {"hv":"1","hh":"2", "vv":"1", "vh":"2"}

WORKING_DIR=S1GRD1SDV.process_dir

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
    log.info("Collecting granules metadata. Number of granules to process: {}".format(len(granules_paths)))
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
                "location": location,
                "band"    : band_number[granule_name.split("-")[3]]
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

def create_owslinks_dict(product_metadata, granules_paths, bbox_str):
    # Get BBOX from on of the granules, assuming same for all of them
    for granule in granules_paths:
        bbox_polygon = get_bbox_from_granule(granule)
        break
    lat_lower_left=bbox_polygon[1][1]
    lat_upper_right=bbox_polygon[3][1]
    long_lower_left=bbox_polygon[1][0]
    long_upper_right=bbox_polygon[3][0]
    log.info("""
    granules bbox:
        long_lower_left: {}
        long_upper_right: {}
        lat_lower_left: {}
        lat_upper_right: {}
    """.format(
        long_lower_left,
        long_upper_right,
        lat_lower_left,
        lat_upper_right
    ))

    owslinks_dict = \
        {
            "links": [
                # WMS Links
                {
                    "offering": "http://www.opengis.net/spec/owc/1.0/req/atom/wms",
                    "method": "GET",
                    "code": "GetCapabilities",
                    "type": "application/xml",
                    "href": "${BASE_URL}" + "/{}/{}/ows?service=WMS&request={}&version={}&CQL_FILTER=eoIdentifier='{}'".format(
                        GS_WMS_WORKSPACE, GS_WMS_LAYER, "GetCapabilities", GS_WMS_VERSION, product_metadata['NAME'])
                },
                {
                    "offering": "http://www.opengis.net/spec/owc/1.0/req/atom/wms",
                    "method": "GET",
                    "code": "GetMap",
                    "type": "image/jpeg",
                    "href": "${BASE_URL}" + "/{}/{}/ows?service=WMS&request={}&version={}&LAYERS={}&BBOX={}&WIDTH={}&HEIGHT={}&FORMAT={}&CQL_FILTER=eoIdentifier='{}'".format(
                        GS_WMS_WORKSPACE, GS_WMS_LAYER, "GetMap", GS_WMS_VERSION, GS_WMS_LAYER, bbox_str, GS_WMS_WIDTH, GS_WMS_HEIGHT,
                        GS_WMS_FORMAT, product_metadata['NAME'])
                },
                # WFS Links
                {
                    "offering": "http://www.opengis.net/spec/owc/1.0/req/atom/wfs",
                    "method": "GET",
                    "code": "GetCapabilities",
                    "type": "application/xml",
                    "href": "${BASE_URL}" + "/{}/{}/ows?service=WFS&request={}&version={}&CQL_FILTER=eoIdentifier='{}'".format(
                        GS_WFS_WORKSPACE, GS_WFS_LAYER, "GetCapabilities", GS_WFS_VERSION, product_metadata['NAME'])
                },
                {
                    "offering": "http://www.opengis.net/spec/owc/1.0/req/atom/wfs",
                    "method": "GET",
                    "code": "GetFeature",
                    "type": "image/jpeg",
                    "href": "${BASE_URL}" + "/{}/{}/ows?service=WFS&request={}&version={}&typeNames={}:{}&outputFormat={}&CQL_FILTER=eoIdentifier='{}'".format(
                        GS_WFS_WORKSPACE, GS_WFS_LAYER, "GetFeature", GS_WFS_VERSION, GS_WFS_WORKSPACE, GS_WFS_LAYER,
                        GS_WFS_FORMAT, product_metadata['NAME'])
                },
                # WCS Links
                {
                    "offering": "http://www.opengis.net/spec/owc/1.0/req/atom/wcs",
                    "method": "GET",
                    "code": "GetCapabilities",
                    "type": "application/xml",
                    "href": "${BASE_URL}" + "/{}/{}/ows?service=WCS&request={}&version={}&CQL_FILTER=eoIdentifier='{}'".format(
                        GS_WCS_WORKSPACE, GS_WCS_LAYER, "GetCapabilities", GS_WCS_VERSION, product_metadata['NAME'])
                },
                {
                    "offering": "http://www.opengis.net/spec/owc/1.0/req/atom/wcs",
                    "method": "GET",
                    "code": "GetCoverage",
                    "type": "image/jpeg",
                    "href": "${BASE_URL}" + "/{}/{}/ows?service=WCS&request={}&version={}&coverageId={}__{}&FORMAT={}&subset=http://www.opengis.net/def/axis/OGC/0/Long({},{})&subset=http://www.opengis.net/def/axis/OGC/0/Lat({},{})&scaleaxes=i({}),j({})&CQL_FILTER=eoIdentifier='{}'".format(
                        GS_WCS_WORKSPACE, GS_WCS_LAYER, "GetCoverage", GS_WCS_VERSION, GS_WCS_WORKSPACE, GS_WCS_LAYER,
                        GS_WCS_FORMAT, long_lower_left, long_upper_right, lat_lower_left, lat_upper_right,
                        GS_WCS_SCALE_I, GS_WCS_SCALE_J, product_metadata['NAME'])
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

def create_procuct_zip(sentinel1_product_zip_path, granules_paths, granules_upload_dir, working_dir=WORKING_DIR):
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
    granules_dict, bbox_str = collect_granules_metadata(granules_paths, granules_upload_dir)
    log.info( "granules_dict: \n{}".format(pprint.pformat(granules_dict)) )
    path = os.path.join(working_dir, 'granules.json')
    with open(path, 'w') as f:
        json.dump(granules_dict, f, indent=4)
    files.append(path)

    # Use the coordinates from the granules in the product.json as the s1reader seems to
    # swap the footprint coordinates, see https://github.com/geosolutions-it/evo-odas/issues/192
    granule_coords=granules_dict.get('features')[0].get('geometry').get('coordinates')
    #print(">>>>>>>>>>>>>{}".format(granule_coords))
    search_params['geometry']['coordinates']=granule_coords
    #print(">>>>>>>>>>>>>{}".format(search_params.get('geometry').get('coordinates')))
    
    # dump product.json to file
    log.info("Creating product.json")
    path = os.path.join(working_dir, 'product.json')
    with open(path, 'w') as f:
        json.dump(search_params, f, indent=4)
    files.append(path)

    # create owslinks.json and dump it to file
    log.info("Creating owslinks.json")
    owslinks_dict = create_owslinks_dict(s1metadata, granules_paths, bbox_str)
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


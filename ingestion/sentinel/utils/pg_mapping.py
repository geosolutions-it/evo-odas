#!/usr/bin/env python

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
                'eoIdentifier':granule.granule_identifier,
                'originalPackageType':"application/zip"
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
            })

def create_ogc_links_dict(list):
    ogc_links = []
    for el in list:
        ogc_links.append({
            'offering':el[1],
            'method':el[2],
            'code':el[3],
            'type':el[4],
            'href':el[5]
        })
    return ogc_links

def ogc_links_href_dict(ogc_bbox, product_id):
    return{
        "WORKSPACE":"sentinel2",
        "LAYER":"sentinel2-TCI",
        "WIDTH":"800",
        "HEIGHT":"600",
        "PRODUCT_ID":product_id,
        "MINX":ogc_bbox[0],
        "MINY":ogc_bbox[1],
        "MAXX":ogc_bbox[2],
        "MAXY":ogc_bbox[3],
        "WCS_SCALEFACTOR":0.01
    }

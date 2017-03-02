import s2reader
import sys
import os
import pg_simple
import metadata_db_connection_params as db_config

def printMetadata(safe_pkg):
    print "--------------------COLLECTION---------------------------------------------"
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
        print "footprint_char_length: '" + str(len(str(granule.footprint)))
        print "srid: '" +granule.srid
        print "metadata_path: '" + granule.metadata_path
        print "cloud_percent: '" + str(granule.cloud_percent)
        #print "cloudmask: '" + str(len(str(granule.cloudmask)))
        print "nodata_mask: '" + str(granule.nodata_mask)
        print "band_path: '" + str(granule.band_path)

        print "--------------------------------------------------------------------"

def collectSentinel2Metadata(safe_pkg, granule):
    return {
        '"timeStart"':safe_pkg.product_start_time,
        '"timeEnd"':safe_pkg.product_stop_time,
        '"eoParentIdentifier"':"S2_MSI_L1C",# from Torsten velocity template see related mail in ML
        '"eoAcquisitionType"':"NOMINAL",# from Torsten velocity template see related mail in ML
        '"eoOrbitNumber"':safe_pkg.sensing_orbit_number,
        '"eoOrbitDirection"':safe_pkg.sensing_orbit_direction,
        '"optCloudCover"':granule.cloud_percent,
        '"eoCreationDate"':safe_pkg.generation_time,
        '"eoArchivingCenter"':"DPA",# from Torsten velocity template see related mail in ML
        '"eoProcessingMode"':"DATA_DRIVEN",# from Torsten velocity template see related mail in ML
    }
    # TO BE USED IN METADATA TEMPLATE
    #'"eoProcessingLevel"':safe_pkg.processing_level,
    #'"footprint"':str(safe_pkg.footprint),
    #'"eoSensorType"':"OPTICAL",
    #'"eoOrbitType"':"LEO",
    #'"eoProductType"':safe_pkg.product_type,
    #'"eoInstrument"':safe_pkg.product_type[2:5],
    #'"eoPlatform"':safe_pkg.spacecraft_name[0:10],
    #'"eoPlatformSerialIdentifier"':safe_pkg.spacecraft_name[10:11]

def persistProduct(dict_to_persist):
    pg_simple.config_pool(max_conn=db_config.max_conn,
                            expiration=db_config.expiration,
                            host=db_config.host,
                            port=db_config.port,
                            database=db_config.database,
                            user=db_config.user,
                            password=db_config.password)
    with pg_simple.PgSimple() as db:
        collection = db.fetchone('metadata.collection',
                                    fields=['"eoIdentifier"'],
                                    where=('"eoIdentifier" = %s', ["SENTINEL2"]))
        dict_to_persist['"eoParentIdentifier"'] = collection.eoIdentifier
        db.insert('metadata.product', data=dict_to_persist)
        db.commit()


def main(args):
    if len(args) > 1:
       raise Error("too many parameters!")
    print "arg is: '" + args[0] + "'"

    with s2reader.open(args[0]) as safe_pkg:
        #printMetadata(safe_pkg)
        for granule in safe_pkg.granules:
            dict_to_persist=collectSentinel2Metadata(safe_pkg, granule)
            persistProduct(dict_to_persist)

if __name__ == "__main__":
    main(sys.argv[1:])

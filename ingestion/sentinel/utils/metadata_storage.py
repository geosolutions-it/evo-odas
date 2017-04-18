#!/usr/bin/env python
import pg_simple
import configs.metadata_db_connection_params as db_config
from psycopg2 import Binary
import configs.workflows_settings as wf

class PostgresStorage:
    """Implementation of the EVO-ODAS Search Engine storage using the Postgres relational DBMS with PostGIS spatial extension"""

    def __init__(self):
        pg_simple.config_pool(max_conn=db_config.max_conn,
                                expiration=db_config.expiration,
                                host=db_config.host,
                                port=db_config.port,
                                database=db_config.database,
                                user=db_config.user,
                                password=db_config.password)
        self.schema = db_config.schema

    def check_granule_identifier(self, granule_identifier):
        with pg_simple.PgSimple() as db:
            collection = db.fetchone(self.schema + '.product',
                                        fields=['"eoIdentifier"'],
                                        where=('"eoIdentifier" = %s', [granule_identifier]))
            if collection == None:
                return False
            else:
                return True

    def get_product_OGC_BBOX(self, granule_identifier):
        with pg_simple.PgSimple(nt_cursor=False) as db:
            result = db.execute('SELECT ST_Extent(footprint::geometry) as ext FROM ' + self.schema + '.product WHERE "eoIdentifier" = %s', [granule_identifier])
            bbox = result.fetchone()[0]
            bbox = bbox[4:-1]
            bbox = bbox.replace(",", " ")
            bbox = bbox.split(" ", -1)
            if float(bbox[0]) > float(bbox[2]):
                tmp = bbox[0]
                bbox[0] = bbox[2]
                bbox[2] = tmp
            if float(bbox[1]) > float(bbox[3]):
                tmp = bbox[1]
                bbox[1] = bbox[3]
                bbox[3] = tmp
            return bbox

    def get_products_id(self):
        with pg_simple.PgSimple() as db:
            id_list = db.fetchall(self.schema + '.product', fields=['"id"', '"eoIdentifier"'])
        return id_list

    def persist_collection(self, dict_to_persist):
        with pg_simple.PgSimple() as db:
            row = db.insert(self.schema + '.collection', data=dict_to_persist)
            db.commit()

    def persist_product_search_params(self, dict_to_persist, collection_name):
        with pg_simple.PgSimple() as db:
            collection = db.fetchone(self.schema + '.collection',
                                        fields=['"eoIdentifier"'],
                                        where=('"eoIdentifier" = %s', [collection_name]))
            if collection is None:
                raise LookupError("ERROR: No related collection found!")
            dict_to_persist['"eoParentIdentifier"'] = collection.eoIdentifier
            row = db.insert(self.schema + '.product', data=dict_to_persist, returning='id')
            db.commit()
        return row.id

    def persist_product_metadata(self, xml_doc, id):
        with pg_simple.PgSimple() as db:
            db.insert(self.schema + '.product_metadata', data={'metadata':xml_doc, 'mid':id})
            db.commit()

    def persist_thumb(self, thumb_blob, id):
        with pg_simple.PgSimple() as db:
            db.insert(self.schema + '.product_thumb', data={'thumb':Binary(thumb_blob), 'tid':id})
            db.commit()

    def persist_ogc_links(self, list, id):
        with pg_simple.PgSimple() as db:
            for dict in list:
                dict['product_id'] = id
                db.insert(self.schema + '.product_ogclink', data=dict)
        db.commit()

    def update_original_package_location(self, safe_pkg_name, tile_id):
        with pg_simple.PgSimple() as db:
            db.update(self.schema + '.product',
                                    data={'"originalPackageLocation"':wf.original_package_location_path + safe_pkg_name.replace("SAFE","zip")},
                                    where=('"eoIdentifier" = %s', [tile_id]))
        db.commit()

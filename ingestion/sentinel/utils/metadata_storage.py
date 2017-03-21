#!/usr/bin/env python
import pg_simple
import configs.metadata_db_connection_params as db_config
from psycopg2 import Binary

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

    def check_granule_identifier(self, granule_identifier):
        with pg_simple.PgSimple() as db:
            collection = db.fetchone('metadata.product',
                                        fields=['"eoIdentifier"'],
                                        where=('"eoIdentifier" = %s', [granule_identifier]))
            if collection == None:
                return False
            else:
                return True

    def persist_product_search_params(self, dict_to_persist, collection_name):
        with pg_simple.PgSimple() as db:
            collection = db.fetchone('metadata.collection',
                                        fields=['"eoIdentifier"'],
                                        where=('"eoIdentifier" = %s', [collection_name]))
            if collection is None:
                raise LookupError("ERROR: No related collection found!")
            dict_to_persist['"eoParentIdentifier"'] = collection.eoIdentifier
            row = db.insert('metadata.product', data=dict_to_persist, returning='id')
            db.commit()
        return row.id

    def persist_product_metadata(self, xml_doc, id):
        with pg_simple.PgSimple() as db:
            db.insert('metadata.product_metadata', data={'metadata':xml_doc, 'id':id})
            db.commit()

    def persist_thumb(self, thumb_blob, id):
        with pg_simple.PgSimple() as db:
            db.insert('metadata.product_thumb', data={'thumb':Binary(thumb_blob), 'id':id})
            db.commit()

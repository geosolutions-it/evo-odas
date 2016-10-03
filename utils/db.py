#!/usr/bin/python

import psycopg2
import os


class DB(object):

    def __init__(self):
        self.db_name = 'ows12'
        self.db_host = 'localhost'
        self.db_port = '5432'
        self.db_user = 'geoserver'
        self.db_passw = 'mypass'

    def initDB(self, args):
        db = DB()
        db.db_name = args[0]
        db.db_host = args[1]
        db.db_port = args[2]
        db.db_user = args[3]
        db.db_passw = args[4]
        return db

    def connect(self):
        conn = psycopg2.connect(database=self.db_name, host=self.db_host, port=self.db_port,
                                user=self.db_user, password=self.db_passw)

        return conn

    def query(self, cursor, band, sceneID):
        try:
            sql = 'SELECT regexp_matches(location, \'%s\') FROM ows12.\"B%s\";' % (sceneID, band)
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) == 0:
                return False
            else:
                return results
        except Exception as e:
            print str(e)

    def ingest(self, cursor, band, row, path):
        try:
            image = os.path.join(path, str(row[2]['properties']['sceneID']),
                                 str(row[2]['properties']['sceneID']) + '_B' + band + '.TIF')
            time = str(row[2]['properties']['date'])
            geom = 'ST_GeomFromGeoJSON(\'{"type": "' + str(row[2]['geometry']['type']) \
                   + '", "coordinates": ' + str(row[2]['geometry']['coordinates']) \
                   + ', "crs":{"type":"name","properties":{"name":"EPSG:4326"}}}\')'
            sql = 'INSERT INTO ows12.\"B%s\" (location, time, the_geom) VALUES (\'%s\', \'%s\', %s);' % (band, image, time, geom)
            cursor.execute(str(sql))
        except Exception as e:
            print str(e)



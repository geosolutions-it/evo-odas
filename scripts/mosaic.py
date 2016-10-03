#!/usr/bin/python

from geoserver.catalog import Catalog


class Mosaic(object):
    def __init__(self):
        self.rest_url = 'http://localhost:8080/geoserver/rest'
        self.username = 'admin'
        self.password = 'geoserver'

    @property
    def catalog(self):
        return Catalog(self.rest_url, username=self.username, password=self.password)


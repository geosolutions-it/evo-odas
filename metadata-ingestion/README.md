
## What's this?

This pyhon library perform the Geoserver + PostGIS open search metadata ingestion for Sentinel 1 and 2 SAFE packages.

It is written in python and it uses:

* [s2 reader](https://github.com/ungarj/s2reader) to extract metadata from Sentinel2 SAFE packages
* [pg_simple](https://github.com/masroore/pg_simple) to deal with PostGIS
* [jinja2](https://github.com/pallets/jinja) for template rendering
* [pgmagick](https://github.com/hhatto/pgmagick) to produce thumbnails

The model used for the storage is defined in [this DDL](https://github.com/geoserver/geoserver/blob/master/src/community/oseo/oseo-core/src/test/resources/postgis.sql)

### Sentinel1

A subset of the information needed by the OpenSearch model is extracted from the SAFE manifest file using a recent version GDAL's Python APIs

## Installation

**Python 2.7.x**, **git** and **pip** are required.

* Clone this repo

* Install few Ubuntu packages required for psycopg2, pgmagick and pycurl python libraries:

```
#$ sudo apt-get install libpq-dev python-dev libgraphicsmagick++1-dev libboost-python-dev curl build-essential libcurl4-openssl-dev
```

* **Only for the Sentinel1 ingestion** latest stable GDAL release (2.1.3 atm) with python bindings is required, here are listed the instrucions to download and install it from the sources:

```
#$ sudo apt-get install python-numpy python-scipy
#$ sudo apt-get install build-essential python-all-dev
#$ cd /tmp
#$ wget http://download.osgeo.org/gdal/2.1.3/gdal-grass-2.1.3.tar.gz
#$ tar xvfz gdal-1.9.1.tar.gz
#$ cd gdal-1.9.1
#$ ./configure --with-python
#$ make
#$ sudo make install
```

* in order to create the metadata database run [this DDL](https://github.com/geoserver/geoserver/blob/master/src/community/oseo/oseo-core/src/test/resources/postgis.sql) against a PostGIS instance

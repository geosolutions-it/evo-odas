
## What's this?

This pyhon library perform the Geoserver + PostGIS open search metadata ingestion for Sentinel 1 and 2 SAFE packages.

It is written in python and it uses:

* [s2 reader](https://github.com/ungarj/s2reader) to extract metadata from Sentinel2 SAFE packages
* [pg_simple](https://github.com/masroore/pg_simple) to deal with PostGIS
* [jinja2](https://github.com/pallets/jinja) for template rendering

The model used for the storage is defined in [this DDL](https://github.com/geoserver/geoserver/blob/master/src/community/oseo/oseo-core/src/test/resources/postgis.sql)

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

* then install all the python dependencies listed in the requirements.txt, use the following command in order to ensure the installation order:

```
#$ sudo xargs -L 1 pip install < requirements.txt
```

* in order to create the metadata database run [this DDL](https://github.com/geoserver/geoserver/blob/master/src/community/oseo/oseo-core/src/test/resources/postgis.sql) against a PostGIS instance

* set the connection parameters to the PostGIS instance in [this config file](https://github.com/geosolutions-it/evo-odas/blob/master/ingestion/sentinel/configs/metadata_db_connection_params.py)

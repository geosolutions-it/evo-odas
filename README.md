[![Stories in Ready](https://badge.waffle.io/geosolutions-it/evo-odas.png?label=ready&title=Ready)](https://waffle.io/geosolutions-it/evo-odas)
# EVO-ODAS 

## Ingestion system
* ingestion of Sentinel2, Sentinel1 and Landsat8 satellite data
* a collection of python scripts
* used Jenkins pipelines as workflow engine

**WORK IN PROGRESS!**

### Scripts

#### Sentinel2

* `ingestion/sentinel`
  * raw data download
  * optimizations and reprojections
  * publication
  * metadata ingestion in a search engine database

#### Landsat8

* `ingestion/lsat`
  * raw data download
  * optimizations and reprojections
  * publication
  
#### Sentinel1

Not supported yet


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

[![Stories in Ready](https://badge.waffle.io/geosolutions-it/evo-odas.png?label=ready&title=Ready)](https://waffle.io/geosolutions-it/evo-odas)
# EVO-ODAS Preprocessing and Ingestion Scripts

## Rationale
This repository contains Python scripts based on GDAL for preprocessing raster data coming from landsat imagery in order to optimize them for publishing as WMS/WMTS.
The scripts are divided into basic functionality (Download Images, Preprocess Images, Harvest Images).

## Installation
The scripts were tested in **Python 2.7** and have some dependencies:
 * **[GDAL python bindings](https://pypi.python.org/pypi/GDAL/)**
 * **[landsat-util](https://pypi.python.org/pypi/landsat-util)**
 * **[sentinelsat](https://github.com/ibamacsr/sentinelsat)**
 * **[psycopg2](https://pypi.python.org/pypi/psycopg2)**
 * **[gsconfig](https://pypi.python.org/pypi/gsconfig)**

## Landsat8

### Download Images

`python lsat/lsat8_getimages.py -h`

#### Arguments
* `-b, --bands` - Scene bands to retrieve. e.g. `-b 4 3 2` (RGB bands).
* `-l, --limit` - Search return results limit. Defaults to `10`.
* `-s, --start` - Start Date - Most formats are accepted. e.g. `-s 06/12/2014`
* `-e, --end` - Start Date - Most formats are accepted. e.g. `-e 06/12/2014`
* `-c, --cloud` - Maximum cloud percentage. Defaults to `10`.
* `-p, --pathrow` - Paths and Rows in order separated by comma. e.g. path,row,path,row `001,001,190,204`
* `--lat` - The latitude of a place to retrieve images.
* `--lon` - The longitude of a place to retrieve images.
* `--address` - The address of a place to retrieve images.
* `--catalog` - Geoserver REST URL and authentication. e.g. `http://myurl/mygeoserver/rest myuser mypass` 
* `--store` - Geoserver store name to manipulate coverages with workspace name. e.g. `ows12:landsat`
* `output` - Filesystem path to save downloaded images.

### Process Images

`python lsat/lsat8_process.py -h`

#### Arguments
* `-b, --bands` - List of individual band images to process. e.g. `-b 4 3 2` (RGB bands).
* `-r, --resample` - Resample method to use on GDAL utils. Default is `nearest`
* `-c, --config` - Specific GDAL configuration string. e.g. `--config COMPRESS_OVERVIEW DEFLATE`
* `-o, --overviews` - Overviews to add to the target image. e.g. `-o "2,4,8,16,32"`
* `-w, --warp` - The target projection EPSG code to use for gdalwarp. e.g. `-w EPSG:4326`
* `output` - Filesystem path where saved images can be retrieved from.

### Harvest Images

`python lsat/lsat8_harvest.py -h`

#### Arguments
* `-c, --catalog` - Geoserver REST URL and authentication. e.g. `-c http://localhost:8080/geoserver/rest  admin  geoserver`
* `-s, --store` - Geoserver store name to manipulate with workspace name. e.g. `-s ows12:landsat`
* `-i, --insert` - Harvest (insert) new granule on a given mosaic store. Default `true`
* `-d, --delete` - Delete granules older than given months value. e.g. `-d 3`
* `--granules` - Filesystem path where saved images can be retrieved from.

## Sentinel2

### Download Images

`python sentinel/ssat_getimages.py -h`

#### Arguments
* `-s, --start` - Start Date - Most formats are accepted. e.g. `-s 06/12/2014`
* `-e, --end` - Start Date - Most formats are accepted. e.g. `-e 06/12/2014`
* `--auth` - DataHub API authentication. e.g. `myuser mypass`
* `--query` - Specific keywords to query SciHub API. e.g. `cloudcoverpercentage:[0 TO 20] platformname:Sentinel-2`
* `--geojson` - Path to Geojson file containing area to search. e.g. `myaoi.geojson` 
* `--store` - Geoserver store name to manipulate coverages with workspace name. e.g. `ows12:landsat`
* `--output` - Filesystem path to save downloaded images.

### Process Images

`python sentinel/ssat_process.py -h`

#### Arguments
* `-b, --bands` - List of individual band images to process. e.g. `-b 4 3 2` (RGB bands).
* `-r, --resample` - Resample method to use on GDAL utils. Default is `nearest`
* `-c, --config` - Specific GDAL configuration string. e.g. `--config COMPRESS_OVERVIEW DEFLATE`
* `-o, --overviews` - Overviews to add to the target image. e.g. `-o "2,4,8,16,32"`
* `-w, --warp` - The target projection EPSG code to use for gdalwarp. e.g. `-w EPSG:4326`
* `--download` - Path to previously downloaded Sentinel2 products packages.
* `--output` - Filesystem path where saved images can be retrieved from.

### Harvest Images

`python sentinel/ssat_harvest.py -h`

#### Arguments
* `-c, --catalog` - Geoserver REST URL and authentication. e.g. `-c http://localhost:8080/geoserver/rest  admin  geoserver`
* `-s, --store` - Geoserver store name to manipulate with workspace name. e.g. `-s ows12:landsat`
* `-i, --insert` - Harvest (insert) new granule on a given mosaic store. Default `true`
* `-d, --delete` - Delete granules older than given months value. e.g. `-d 3`
* `--granules` - Filesystem path where saved images can be retrieved from.
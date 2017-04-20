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

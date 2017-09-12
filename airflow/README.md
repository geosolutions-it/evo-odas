# Airflow

## Directory Structure

* The `dags` and `plugins` folders are structured to be directly imported in the [Airflow Home directory](https://airflow.incubator.apache.org/start.html#quick-start).
They can be copied or linked inside the home directory.

* The `docker` folder contain a docker file to build up a docker image with an airflow test environment. To test

* The `config` folder contain configuration items as python scripts that can be overriden by environment specific files in `config/override`.


## Testing

For development and testing it is helpful to mount folders into the docker container



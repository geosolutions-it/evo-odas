# Airflow

## Directory Structure

* The `dags` and `plugins` folders are structured to be directly imported in the [Airflow Home directory](https://airflow.incubator.apache.org/start.html#quick-start).
They can be copied or linked inside the home directory.

* The `docker` folder contain a docker file to build up a docker image with an airflow test environment. To test

* The `config` folder contain configuration items as python scripts that can be overriden by environment specific files in `config/override`.


## Testing

For development and testing it might be helpful to quickly start a local container and do some test:

```
# Source the script to add the commands to your shell
$ source myairflow

# Create some local, relative paths needed to run the container
$ airflow_init

# Build the docker image
$ airflow_build

# Start the docker container
$ airflow_start

# Run some predefined tests (see myairflow) within the container
$ airflow_test

# Run a specific test within the container
$ airflow_exec airflow test S2_MSI_L1C search_product_task 2017-01-01

# For other useful commands, see:
$ ./myairflow help
USAGE: ./myairflow [status|exec|init|test|update|build|start|stop|restart|remove|login|logs]
```


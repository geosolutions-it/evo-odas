# Usage

```bash
IMAGE_NAME="airflow_testing"

echo "Building Docker image $IMAGE_NAME"
docker build -t $IMAGE_NAME .

echo "Running Docker image $IMAGE_NAME"
#Adjust local paths accordingly
docker run \
    -v `pwd`/../../:/usr/local/evoodas/geosolutions/ \
    -v `pwd`/../../../../testing//:/var/data/ \
    -d -p 8080:8080 \
    $IMAGE_NAME
```

Navigate to http://localhost:8080 in your browser

To examine the container use the following

```bash
docker ps
docker exec -it <CONTAINER ID> /bin/bash
docker stop <CONTAINER ID>
```

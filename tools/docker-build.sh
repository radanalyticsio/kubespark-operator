

export REMOTE_IMAGE=docker.io/zmhassan/kubespark-operator
export LOCAL_IMAGE=zmhassan/kubespark-operator


echo   " Building Spark Operator in Docker "
docker build --rm -t   $LOCAL_IMAGE . 

echo "Pushing containers up to docker hub"
#docker tag $LOCAL_IMAGE $REMOTE_IMAGE
docker push $REMOTE_IMAGE


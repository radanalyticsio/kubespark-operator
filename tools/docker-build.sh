echo   " Building Spark Operator in Docker "
docker   build  --rm -t  zmhassan/oshinko-crd  ../
echo "Pushing containers up to docker hub"
docker push zmhassan/oshinko-crd


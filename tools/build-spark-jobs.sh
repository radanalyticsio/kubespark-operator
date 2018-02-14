#!/bin/sh

VERSION="1.0.0-SNAPSHOT"
AUTHOR="Zak Hassan <zak.hassan1010@gmail.com>"
COMPONENT="Spark Job Operator"

cat ../Banner.txt

echo "Version: $VERSION"
echo "Author: $AUTHOR"
echo "Component: $COMPONENT"


echo "Building Spark Operator ..."
echo ""
rm -rf ../_output
go build -o ../_output/spark-job-crd ../spark-job-operator.go
echo ""
echo "Status : Complete. Check _output directory"
echo ""
echo "To run execute: ./_output/spark-job-crd"

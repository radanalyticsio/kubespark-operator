.PHONY : build clean install test validate-api image generate

build:
	tools/build.sh build

clean:
	rm -rf _output

install:
	tools/build.sh install

test:
	tools/oshinko-test.sh -t unit

image:
	docker build -t oshinko-rest .

build-cluster-operator:
    go build -o _output/spark-crd spark-cluster-operator.go

build-job-operator:
    go build -o _output/spark-job-crd spark-job-operator.go


build-cli:
	go build -o _output/oshinko-cli ./cli/main.go

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

build-cli:
	go build -o _output/oshinko-cli ./cli/main.go

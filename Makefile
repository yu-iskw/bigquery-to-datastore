#
# Copyright (c) 2017 Yu Ishikawa.
#

DOCKER_TAG := yuiskw/bigquery-to-datastore:latest
DOCKER_TAG_VERSION := yuiskw/bigquery-to-datastore:$(shell bash ./dev/get-app-version.sh)-beam-2.1

.PHONY: test checkstyle clean

all: checkstyle test package

package:
		mvn clean package

test:
		mvn test
		bash ./dev/test.sh

checkstyle:
		mvn checkstyle:checkstyle

clean:
	  mvn clean

build-docker: package
	docker build --rm -t $(DOCKER_TAG) .
	docker build --rm -t $(DOCKER_TAG_VERSION) .

test-docker: build-docker
	docker run --rm --entrypoint "bash" $(DOCKER_TAG) ./dev/test.sh

push-docker: push-docker-latest push-docker-version

push-docker-latest:
	docker push $(DOCKER_TAG)

push-docker-version:
	docker push $(DOCKER_TAG_VERSION)

#
# Copyright (c) 2017 Yu Ishikawa.
#

DOCKER_REPO := yuiskw/bigquery-to-datastore

.PHONY: test checkstyle clean \
		build-docker test-docker push-docker push-docker-latest push-docker-version

all: checkstyle test package

package:
		mvn clean package

test:
		mvn test

test-command:
		bash ./dev/test.sh

checkstyle:
		mvn checkstyle:checkstyle

clean:
	  mvn clean

build-docker: build-docker-latest build-docker-version

build-docker-latest: package
	docker build --rm -t $(DOCKER_REPO):latest .

build-docker-version: check-docker-image-tag package
	docker build --rm -t $(DOCKER_REPO):$(DOCKER_IMAGE_TAG) .

test-docker: build-docker
	docker run --rm --entrypoint "bash" $(DOCKER_TAG) ./dev/test.sh

push-docker: check-docker-image-tag
	docker push $(DOCKER_REPO):$(DOCKER_TAG_TAG)

check-docker-image-tag:
ifndef DOCKER_IMAGE_TAG
	$(error DOCKER_IMAGE_TAG is undefined)
endif

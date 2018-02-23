FROM openjdk:8-slim

ENV PROJECT_ROOT=/root
WORKDIR ${PROJECT_ROOT}

# build
COPY . ${PROJECT_ROOT}

ENTRYPOINT ["./bin/bigquery-to-datastore"]

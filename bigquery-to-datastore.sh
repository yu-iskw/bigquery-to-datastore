#!/bin/bash

ARGS=$@

MAIN_CLASS='com.github.yuiskw.beam.BigQuery2Datastore'
mvn compile exec:java -Dexec.mainClass=${MAIN_CLASS} \
  -Dexec.args="$ARGS" \
  -Pdirect-runner

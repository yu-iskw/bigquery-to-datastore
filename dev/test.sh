#
# Copyright (c) 2017 Yu Ishikawa.
#
PROJECT_DIR=$(dirname $(dirname $(readlink -f $0)))

# check if the head line is the same as the expected or not.
RESULT=$(bash "${PROJECT_DIR}/bin/bigquery-to-datastore" --help | head -n 1)
EXPECTED='com.github.yuiskw.beam.BigQuery2Datastore$Options:'
if [ "$RESULT" != "$EXPECTED" ] ; then
  echo "ERROR: Please check: ./bin/bigquery-to-datastore"
  exit 1
fi

echo "Pass all tests in $(readlink -f $0)"
exit 0

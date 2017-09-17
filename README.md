# `bigquery-to-datastore`

This allows us to export a BigQuery table to a Google Datastore kind using Apache Beam on top of Google Dataflow.

You don't have to have duplicated rows whose key values are same.
Apache Beam's `DatastoreIO` doesn't allow us to write same key at once.

![Data Pipeline](./docs/flow.png)

## Requirements

- Maven
- Java 1.7+
- Google Cloud Platform account

## How to Use

### Command Line Options

#### Required Options
- `--project`: Google Cloud Project
- `--inputBigQueryDataset`: Input BigQuery dataset ID
- `--inputBigQueryTable`: Input BigQuery table ID
- `--keyColumn`: BigQuery column name for a key of Google Datastore kind
- `--outputDatastoreNamespace`: Output Google Datastore namespace
- `--outputDatastoreKind`: OUtput Google Datastore kind
- `--tempLocation`: The Cloud Storage path to use for temporary files. Must be a valid Cloud Storage URL, beginning with `gs://`.
- `--gcpTempLocation`: A GCS path for storing temporary files in GCP.

#### Optional Options
- `--runner`: Apache Beam runner.
  - When you don't set this option, it will run on your local machine, not Google Dataflow.
  - e.g. `DataflowRunner`
- `--parentPaths`: Output Google Datastore parent path(s)
  - e.g. `Parent1:p1,Parent2:p2` ==> `KEY('Parent1', 'p1', 'Parent2', 'p2')`
- `--numWorkers`: The number of workers when you run it on top of Google Dataflow.

#### Example to run on Google Dataflow

```
./bigquery-to-datastore.sh \
  --project=${GCP_PROJECT_ID} \
  --runner=DataflowRunner \
  --inputBigQueryDataset=test_dataset \
  --inputBigQueryTable=test_table \
  --outputDatastoreNamespace=test_namespace \
  --outputDatastoreKind=TestKind \
  --parentPaths=Parent1:p1,Parent2:p2 \
  --keyColumn=uuid \
  --tempLocation=gs://test_yu/test-log/ \
  --gcpTempLocation=gs://test_yu/test-log/
```

Or, the below command allows us to run this package with a JAR file.

```
# compile
mvn clean package

# Run bigquery-to-datastore via the compiled JAR file
java -cp /path/to/bigquery-to-datastore-bundled-{version}.jar \
  com.github.yuiskw.beam.BigQuery2Datastore \
  --project=sage-shard-740 \
  --runner=DataflowRunner \
  --inputBigQueryDataset=test_dataset \
  --inputBigQueryTable=test_table \
  --outputDatastoreNamespace=test_namespace \
  --outputDatastoreKind=TestKind \
  --parentPaths=Parent1:p1,Parent2:p2 \
  --keyColumn=id \
  --tempLocation=gs://test_bucket/test-log/ \
  --gcpTempLocation=gs://test_bucket/test-log/
```

### Type conversions between BigQuery and Google Datastore

The below table describes the type conversions between BigQuery and Google Datastore.
Since Datastore unfortunately doesn't have any data type for time, `bigquery-to-datastore` ignore BigQuery columns whose data type are `TIME`.


| BigQuery | Datastore |
|---|---|
| BOOLEAN  | bool  |
| INTEGER  | int |
| DOUBLE  | double  |
| STRING  | string  |
| TIMESTAMP  | timestamp  |
| DATE  | timestamp  |
| TIME  | **ignored: Google Datastore doesn't have time type.**  |
| RECORD  | array  |
| STRUCT  | `Entity`  |

package com.github.yuiskw.beam;

import java.util.LinkedHashMap;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import org.apache.beam.sdk.testing.TestPipeline;

public class BigQuery2DatastoreTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testGetOptions() {
    String[] args = {
        "--project=test-project-id",
        "--inputBigQueryDataset=test_dataset",
        "--inputBigQueryTable=test_table",
        "--outputDatastoreNamespace=test_namespace",
        "--outputDatastoreKind=TestKind",
        "--keyColumn=key_column"
    };
    BigQuery2Datastore.Optoins options = BigQuery2Datastore.getOptions(args);
    assertEquals("test-project-id", options.getProject());
    assertEquals("test_dataset", options.getInputBigQueryDataset());
    assertEquals("test_table", options.getInputBigQueryTable());
    assertEquals("test_namespace", options.getOutputDatastoreNamespace());
    assertEquals("TestKind", options.getOutputDatastoreKind());
    assertEquals("key_column", options.getKeyColumn());
  }

  /**
   Test Query

   SELECT
     "uuid1" AS uuid,
     False AS bool_value,
     1 AS int_value,
     1.23 AS float_value,
     "hoge" AS string_value,
     CURRENT_DATE() AS date_value,
     CURRENT_TIME() AS time_value,
     CURRENT_TIMESTAMP() AS timestamp_value,
     [1, 2] AS int_array_value,
     [1.23, 2.34, 3.45] AS float_array_value,
     ["hoge", "fuga", "hoge2", "fuga2"] AS string_array_value,
     STRUCT(
     1 AS int_value,
     1.23 AS float_value,
     "hoge" AS string_value
     ) AS nested
   UNION ALL
   SELECT
     "uuid2" AS uuid,
     False AS bool_value,
     1 AS int_value,
     1.23 AS float_value,
     "hoge" AS string_value,
     CURRENT_DATE() AS date_value,
     CURRENT_TIME() AS time_value,
     CURRENT_TIMESTAMP() AS timestamp_value,
     [1, 2] AS int_array_value,
     [1.23, 2.34, 3.45] AS float_array_value,
     ["hoge", "fuga", "hoge2", "fuga2"] AS string_array_value,
     STRUCT(
     1 AS int_value,
     1.23 AS float_value,
     "hoge" AS string_value
     ) AS nested
   */
  @Ignore
  public void testMain1() {
    String[] args = {
        "--project=test-project-id",
        "--inputBigQueryDataset=test_yu",
        "--inputBigQueryTable=test_table",
        "--outputDatastoreNamespace=test_double",
        "--outputDatastoreKind=TestKind",
        "--keyColumn=uuid",
        "--tempLocation=gs://test_yu/test-log/",
        "--gcpTempLocation=gs://test_yu/test-log/"
    };
    BigQuery2Datastore.main(args);
  }

  @Ignore
  public void testMain2() {
    String[] args = {
        "--project=test-project-id",
        "--inputBigQueryDataset=test_yu",
        "--inputBigQueryTable=test_table",
        "--outputDatastoreNamespace=test_double",
        "--outputDatastoreKind=TestKind",
        "--parentPaths=Parent1:p1,Parent2:p2",
        "--keyColumn=uuid",
        "--tempLocation=gs://test_yu/test-log/",
        "--gcpTempLocation=gs://test_yu/test-log/"
    };
    BigQuery2Datastore.main(args);
  }

  @Test
  public void testParseParentPaths() {
    String parentPaths = "Parent1:p1,Parent2:p2";
    LinkedHashMap<String, String> parents =
        BigQuery2Datastore.parseParentPaths(parentPaths);
    assertEquals(2, parents.size());
    assertEquals("p1", parents.get("Parent1"));
    assertEquals("p2", parents.get("Parent2"));
  }
}
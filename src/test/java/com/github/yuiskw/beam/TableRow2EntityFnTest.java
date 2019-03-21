/**
 * Copyright (c) 2017 Yu Ishikawa.
 */
package com.github.yuiskw.beam;

import java.text.ParseException;
import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;
import com.google.api.services.bigquery.model.TableRow;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.protobuf.Timestamp;
import org.joda.time.DateTime;
import org.joda.time.Instant;

public class TableRow2EntityFnTest {

  private String projectId = "sage-shard-740";
  private String namespace = "test_double";
  private String kind = "TestKind";
  private String keyColumn = "uuid";
  private List<String> indexedColumns = Arrays.asList("user_id", "name");

  private TableRow getTestTableRow() {
    TableRow row = new TableRow();
    String timestamp = Instant.now().toString();
    Instant.parse(timestamp);
    row.set("uuid", "")
        .set("user_id", 123)
        .set("long_value", 1L)
        .set("name", "abc")
        .set("bool_value", true)
        .set("float_value", 1.23)
        .set("date", new Date())
        .set("datetime", new DateTime(new Date()))
        .set("ts", timestamp)
        .set("child", new TableRow().set("hoge", "fuga"));
    return row;
  }

  @Test
  public void testConvert1() {
    TableRow row = getTestTableRow();

    try {
      TableRow2EntityFn fn =new TableRow2EntityFn(
          projectId, namespace, null, kind, keyColumn, indexedColumns);
      Entity entity = fn.convertTableRowToEntity(row);
      Key key = entity.getKey();
      assertEquals(key.getPartitionId().getProjectId(), projectId);
      assertEquals(key.getPartitionId().getNamespaceId(), namespace);
      assertEquals(key.getPath(0).getKind(), kind);

      Map<String, Value> properties = entity.getPropertiesMap();
      assertFalse(properties.get("user_id").getExcludeFromIndexes());
      assertTrue(properties.get("long_value").getExcludeFromIndexes());
      assertFalse(properties.get("name").getExcludeFromIndexes());
      assertTrue(properties.get("bool_value").getExcludeFromIndexes());
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testConvert2() {
    TableRow row = getTestTableRow();

    try {
      LinkedHashMap<String, String> parents =
          BigQuery2Datastore.parseParentPaths("Parent1:p1,Parent2:p2");
      TableRow2EntityFn fn =
          new TableRow2EntityFn(projectId, namespace, parents, kind, keyColumn, indexedColumns);
      Entity entity = fn.convertTableRowToEntity(row);
      Key key = entity.getKey();
      assertEquals(key.getPartitionId().getProjectId(), projectId);
      assertEquals(key.getPartitionId().getNamespaceId(), namespace);
      assertEquals(key.getPath(0).getKind(), "Parent1");
      assertEquals(key.getPath(1).getKind(), "Parent2");
      assertEquals(key.getPath(2).getKind(), kind);

      Map<String, Value> properties = entity.getPropertiesMap();
      assertFalse(properties.get("user_id").getExcludeFromIndexes());
      assertTrue(properties.get("long_value").getExcludeFromIndexes());
      assertFalse(properties.get("name").getExcludeFromIndexes());
      assertTrue(properties.get("bool_value").getExcludeFromIndexes());
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testConvertToDatastoreValue() {
    TableRow2EntityFn fn =new TableRow2EntityFn(
        projectId, namespace, null, kind, keyColumn, indexedColumns);

    Value v = null;

    // String
    v = fn.convertToDatastoreValue("value", "hello, world");
    assertEquals("hello, world", v.getStringValue());

    // Integer
    v = fn.convertToDatastoreValue("value", 123);
    assertEquals(123, v.getIntegerValue());

    // Double
    v = fn.convertToDatastoreValue("value", 123.456);
    assertEquals(123.456, v.getDoubleValue(), 1e-3);

    // Timestamp
    v = fn.convertToDatastoreValue("value", "2018-01-01 01:23:45");
    assertEquals(1514769825, v.getTimestampValue().getSeconds());

    // Date
    v = fn.convertToDatastoreValue("value", "2018-01-01");
    assertEquals(1514764800, v.getTimestampValue().getSeconds());

    // Array
    v = fn.convertToDatastoreValue("value", Arrays.asList(1, 2, 3));
    assertEquals(3, v.getArrayValue().getValuesCount());

    // Struct
    Map<String, Object> subMap = new HashMap<String, Object>();
    subMap.put("int", 123);
    subMap.put("array", Arrays.asList(1, 2, 3));

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("int", 123);
    map.put("double", 123.456);
    map.put("string", "hello, world");
    map.put("timestamp", "2018-01-01 01:23:45");
    map.put("date", "2018-01-01");
    map.put("array", Arrays.asList(1, 2, 3));
    map.put("struct", subMap);

    v = fn.convertToDatastoreValue("value", map);
    Entity entity = v.getEntityValue();
    assertEquals(123, entity.getPropertiesOrThrow("int").getIntegerValue());
    assertEquals(123.456, entity.getPropertiesOrThrow("double").getDoubleValue(), 1e-3);
    assertEquals("hello, world", entity.getPropertiesOrThrow("string").getStringValue());
    assertEquals(1514769825, entity.getPropertiesOrThrow("timestamp").getTimestampValue().getSeconds());
    assertEquals(1514764800, entity.getPropertiesOrThrow("date").getTimestampValue().getSeconds());
    assertEquals(3, entity.getPropertiesOrThrow("array").getArrayValue().getValuesCount());

    Entity subEntity = entity.getPropertiesOrThrow("struct").getEntityValue();
    assertEquals(123, subEntity.getPropertiesOrThrow("int").getIntegerValue());
    assertEquals(3, subEntity.getPropertiesOrThrow("array").getArrayValue().getValuesCount());
  }

  @Test
  public void testIsDate() {
    assertNotNull(TableRow2EntityFn.parseDate("2017-01-01"));
    assertNotNull(TableRow2EntityFn.parseDate("2017-1-1"));
    assertNotNull(TableRow2EntityFn.parseDate("2017-01-1"));
    assertNotNull(TableRow2EntityFn.parseDate("2017-1-01"));
    assertNotNull(TableRow2EntityFn.parseDate("2017-02-30"));
    assertNull(TableRow2EntityFn.parseDate("hoge"));
  }

  @Test
  public void testIsTimestamp() {
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-09-16 04:14:37.844 UTC"));
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-9-1 4:1:1.1 UTC"));
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-9-1 4:1:1.12 UTC"));
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-9-1 4:1:1.001 UTC"));
    assertNotNull(TableRow2EntityFn.parseTimestamp("2019-03-13 22:00:20 UTC"));

    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-09-16 04:14:37.844 PST"));
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-09-16 04:14:37.844 JST"));

    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-09-16T04:14:37.844"));
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-9-01T4:1:1.1"));
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-9-01T4:1:1.12"));
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-9-01T4:1:1.001"));
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-09-16 04:14:37"));
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-9-01 4:02:03"));

    assertNull(TableRow2EntityFn.parseDate("98-9-12.com"));
    assertNull(TableRow2EntityFn.parseTimestamp("hoge"));
  }

  @Test
  public void testIsExlucedFromIndex() {
    List<String> indexedColumns = Arrays.asList("col1", "col3");
    assertFalse(TableRow2EntityFn.isExcludedFromIndex("col1", indexedColumns));
    assertTrue(TableRow2EntityFn.isExcludedFromIndex("col2", indexedColumns));
    assertFalse(TableRow2EntityFn.isExcludedFromIndex("col3", indexedColumns));

    assertTrue(TableRow2EntityFn.isExcludedFromIndex(null, indexedColumns));
  }

  @Test
  public void testToTimestamp() {
    java.time.Instant instant = java.time.Instant.now();
    Timestamp timestamp = TableRow2EntityFn.toTimestamp(instant);
    assertEquals(instant.getEpochSecond(), timestamp.getSeconds());
  }
}
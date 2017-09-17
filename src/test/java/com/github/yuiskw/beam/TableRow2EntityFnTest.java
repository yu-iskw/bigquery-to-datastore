package com.github.yuiskw.beam;

import java.text.ParseException;
import java.util.Date;
import java.util.LinkedHashMap;

import org.junit.Test;
import static org.junit.Assert.*;
import com.google.api.services.bigquery.model.TableRow;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import org.joda.time.DateTime;
import org.joda.time.Instant;

public class TableRow2EntityFnTest {

  private String projectId = "sage-shard-740";
  private String namespace = "test_double";
  private String kind = "TestKind";
  private String keyColumn = "uuid";

  private TableRow getTestTableRow() {
    TableRow row = new TableRow();
    String hoge = "abc";
    String timestamp = Instant.now().toString();
    Instant.parse(timestamp);
    row.set("uuid", "")
        .set("user_id", 123)
        .set("long_value", 1L)
        .set("name", "abc")
        .set("bool_value", true)
        .set("z", hoge)
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
      TableRow2EntityFn fn = new TableRow2EntityFn(projectId, namespace, null, kind, keyColumn);
      Entity entity = fn.convertTableRowToEntity(row);
      Key key = entity.getKey();
      assertEquals(key.getPartitionId().getProjectId(), projectId);
      assertEquals(key.getPartitionId().getNamespaceId(), namespace);
      assertEquals(key.getPath(0).getKind(), kind);
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
      TableRow2EntityFn fn = new TableRow2EntityFn(projectId, namespace, parents, kind, keyColumn);
      Entity entity = fn.convertTableRowToEntity(row);
      Key key = entity.getKey();
      assertEquals(key.getPartitionId().getProjectId(), projectId);
      assertEquals(key.getPartitionId().getNamespaceId(), namespace);
      assertEquals(key.getPath(0).getKind(), "Parent1");
      assertEquals(key.getPath(1).getKind(), "Parent2");
      assertEquals(key.getPath(2).getKind(), kind);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testIsDate() {
    assertNotNull(TableRow2EntityFn.parseDate("2017-01-01"));
    assertNotNull(TableRow2EntityFn.parseDate("2017-1-1"));
    assertNull(TableRow2EntityFn.parseDate("hoge"));
  }

  @Test
  public void testIsTime() {
    assertNotNull(TableRow2EntityFn.parseTime("04:14:37.844024"));
    assertNotNull(TableRow2EntityFn.parseTime("4:4:7.4"));
    assertNotNull(TableRow2EntityFn.parseTime("04:14:37"));
    assertNull(TableRow2EntityFn.parseTime("hoge"));
  }

  @Test
  public void testIsTimestamp() {
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-09-16 04:14:37.844024 UTC"));
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-09-16 04:14:37.844024 PST"));
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-09-16 04:14:37.844024 JST"));
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-9-16 4:14:37.844024 UTC"));
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-09-16T04:14:37.844024"));
    assertNotNull(TableRow2EntityFn.parseTimestamp("2017-09-16 04:14:37"));
    assertNull(TableRow2EntityFn.parseTimestamp("hoge"));
  }
}
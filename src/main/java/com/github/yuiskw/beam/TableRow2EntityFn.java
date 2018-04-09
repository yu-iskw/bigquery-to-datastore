/**
 * Copyright (c) 2017 Yu Ishikawa.
 */
package com.github.yuiskw.beam;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.*;

import com.google.api.services.bigquery.model.TableRow;
import com.google.datastore.v1.*;
import com.google.protobuf.Timestamp;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;


/**
 * This class is an Apache Beam function to convert TableRow to Entity.
 */
public class TableRow2EntityFn extends DoFn<TableRow, Entity> {

  /** Google Cloud Platform project ID */
  private String projectId;
  /** Google Datastore name space */
  private String namespace;
  /** Google Datastore parent paths */
  private LinkedHashMap<String, String> parents;
  /** Google Datastore kind name */
  private String kind;
  /** BigQuery column for Google Datastore key */
  private String keyColumn;
  /** Indexed columns in Google Datastore */
  private List<String> indexedColumns;

  public TableRow2EntityFn(
      String projectId,
      String namespace,
      LinkedHashMap<String, String> parents,
      String kind,
      String keyColumn,
      List<String> indexedColumns) {
    this.projectId = projectId;
    this.namespace = namespace;
    this.parents = parents;
    this.kind = kind;
    this.keyColumn = keyColumn;
    this.indexedColumns = indexedColumns;
  }

  /**
   * Convert Date to Timestamp
   */
  public static Timestamp toTimestamp(Date date) {
    long millis = date.getTime();
    Timestamp timestamp = Timestamp.newBuilder()
        .setSeconds(millis / 1000)
        .setNanos((int) ((millis % 1000) * 1000000))
        .build();
    return timestamp;
  }

  /**
   * Convert TableRow to Entity
   */
  @ProcessElement
  public void processElement(ProcessContext c) {
    try {
      TableRow row = c.element();
      Entity entity = null;
      entity = convertTableRowToEntity(row);
      c.output(entity);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  /**
   * Convert an object to Datastore value
   */
  public Value convertToDatastoreValue(String columnName, Object value) {
    Value v = null;

    if (value == null) {
      return v;
    }

    // The property is excluded from index or not.
    boolean isExcluded = isExcludedFromIndex(columnName, this.indexedColumns);

    if (value instanceof java.lang.Boolean) {
      v = Value.newBuilder().setBooleanValue(((Boolean) value).booleanValue())
          .setExcludeFromIndexes(isExcluded).build();
    }
    // INTEGER
    else if (value instanceof java.lang.Integer) {
      v = Value.newBuilder().setIntegerValue(((Integer) value).intValue())
          .setExcludeFromIndexes(isExcluded).build();
    }
    else if (value instanceof String && parseInteger((String) value) != null) {
      Integer integer = parseInteger((String) value);
      v = Value.newBuilder().setIntegerValue(integer.intValue())
          .setExcludeFromIndexes(isExcluded).build();
    }
    // LONG
    else if (value instanceof java.lang.Long) {
      v = Value.newBuilder().setIntegerValue((int) ((Long) value).longValue())
          .setExcludeFromIndexes(isExcluded).build();
    }
    // DOUBLE
    else if (value instanceof java.lang.Double) {
      v = Value.newBuilder().setDoubleValue(((Double) value).doubleValue())
          .setExcludeFromIndexes(isExcluded).build();
    }
    // TIMESTAMP
    else if (value instanceof org.joda.time.LocalDateTime) {
      Timestamp timestamp = toTimestamp(((LocalDateTime) value).toLocalDate().toDate());
      v = Value.newBuilder().setTimestampValue(timestamp)
          .setExcludeFromIndexes(isExcluded).build();
    }
    else if (value instanceof String && parseTimestamp((String) value) != null) {
      Date date = parseTimestamp((String) value);
      Timestamp timestamp = toTimestamp(date);
      v = Value.newBuilder().setTimestampValue(timestamp)
          .setExcludeFromIndexes(isExcluded).build();
    }
    // DATE
    else if (value instanceof org.joda.time.LocalDate) {
      Timestamp timestamp = toTimestamp(((LocalDate) value).toDate());
      v = Value.newBuilder().setTimestampValue(timestamp)
          .setExcludeFromIndexes(isExcluded).build();
    } else if (value instanceof String && parseDate((String) value) != null) {
      Date date = parseDate((String) value);
      Timestamp timestamp = toTimestamp(date);
      v = Value.newBuilder().setTimestampValue(timestamp)
          .setExcludeFromIndexes(isExcluded).build();
    }
    // TIME
    // NOTE: Datastore doesn't have any data type to time.
    else if (value instanceof org.joda.time.LocalTime) {
      ;
    } else if (value instanceof String && parseTime((String) value) != null) {
      ;
    }
    // STRING
    else if (value instanceof String) {
      v = Value.newBuilder().setStringValue((String) value)
          .setExcludeFromIndexes(isExcluded).build();
    }
    // RECORD
    else if (value instanceof List) {
      ArrayValue.Builder arrayValueBuilder = ArrayValue.newBuilder();
      List<Object> records = (List<Object>) value;
      for (Object record : records) {
        Value subV = convertToDatastoreValue(columnName, record);
        if (subV != null) {
          arrayValueBuilder.addValues(subV);
        }
      }
      v = Value.newBuilder().setArrayValue(arrayValueBuilder.build()).build();
    }
    // STRUCT
    else if (value instanceof Map) {
      Entity.Builder subEntityBuilder = Entity.newBuilder();
      Map<String, Object> struct = (Map<String, Object>) value;
      for (String subKey : struct.keySet()) {
        Value subV = convertToDatastoreValue(columnName, struct.get(subKey));
        if (subV != null) {
          subEntityBuilder.putProperties(subKey, subV);
        }
      }
      v = Value.newBuilder().setEntityValue(subEntityBuilder.build()).build();
    }
    return v;
  }

  /**
   * Convert TableRow to Entity
   *
   * @param row TableRow of bigquery
   * @return converted Entity
   * @throws ParseException
   */
  public Entity convertTableRowToEntity(TableRow row) throws ParseException {
    String keyName = row.get(keyColumn).toString();
    Key key = getKey(keyName);
    Entity.Builder builder = Entity.newBuilder().setKey(key);

    Set<Map.Entry<String, Object>> entries = row.entrySet();
    for (Map.Entry<String, Object> entry : entries) {
      // Skip on the key column
      if (entry.getKey().equals(keyColumn)) {
        continue;
      }

      // Put a value in the builder
      String propertyName = entry.getKey();
      Object value = entry.getValue();
      Value v = convertToDatastoreValue(propertyName, value);
      if (v != null) {
        builder.putProperties(propertyName, v);
      }
    }
    return builder.build();
  }

  /**
   * Get key
   */
  public Key getKey(String name) {
    Key.Builder keyBuilder = Key.newBuilder();

    // Set namespace
    PartitionId partitionId = PartitionId.newBuilder()
        .setProjectId(projectId).setNamespaceId(namespace).build();
    keyBuilder.setPartitionId(partitionId);

    // Set parent paths
    if (parents != null && parents.size() > 0) {
      for (String parentKey : parents.keySet()) {
        Key.PathElement.Builder parentPath =
            Key.PathElement.newBuilder().setKind(parentKey).setName(parents.get(parentKey));
        keyBuilder.addPath(parentPath);
      }
    }

    // Set main kind
    Key.PathElement.Builder path = Key.PathElement.newBuilder().setKind(kind).setName(name);
    keyBuilder.addPath(path);

    return keyBuilder.build();
  }

  /**
   * Parse integer value
   *
   * @param value String
   * @return parsed integer of null if given value is not integer
   */
  public static Integer parseInteger(String value) {
    Integer integer = null;
    try {
      integer = Integer.valueOf(value);
    } catch (NumberFormatException e) {
      // Do nothing.
      ;
    }
    return integer;
  }

  /**
   * Parse string on date format
   *
   * Note: SimpledateFormat("yyyy-MM-dd") can't help parseing "yyyy-MM-dd HH:mm:ss" as well.
   */
  public static Date parseDate(String value) {
    Date date = null;
    try {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-M-d")
          .withResolverStyle(ResolverStyle.SMART);
      java.time.LocalDate localDate = java.time.LocalDate.parse(value, formatter);
      date = Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
    } catch (DateTimeParseException e) {
      // Do nothing.
      ;
    }
    return date;
  }

  /**
   * Parse string on time format
   */
  public static Date parseTime(String value) {
    Date date = null;
    List<String> patterns = Arrays.asList(
        "HH:mm:ss.SSS z",
        "HH:mm:ss.SSS",
        "HH:mm:ss"
    );
    for (String pattern : patterns) {
      try {
        DateFormat sourceFormat = new SimpleDateFormat(pattern);
        date = sourceFormat.parse(value);
        return date;
      } catch (ParseException e) {
        // Do nothing.
        ;
      }
    }
    return date;
  }

  /**
   * Parse string on timestamp format
   */
  public static Date parseTimestamp(String value) {
    Date date = null;
    List<String> patterns = Arrays.asList(
        "yyyy-MM-dd HH:mm:ss.SSS z",
        "yyyy-MM-dd HH:mm:ss.SSS",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss"
    );
    for (String pattern : patterns) {
      try {
        DateFormat sourceFormat = new SimpleDateFormat(pattern);
        date = sourceFormat.parse(value);
        return date;
      } catch (ParseException e) {
        // Do nothing.
        ;
      }
    }
    return date;
  }

  /**
   * Get if a column is excluded from index or not.
   *
   * @param columnName column name
   * @return if a column is indexed, then return true. Otherwise, return false.
   */
  public static boolean isExcludedFromIndex(String columnName, List<String> indexedColumns) {
    if (indexedColumns.contains(columnName)) {
      return false;
    }
    else {
      return true;
    }
  }
}

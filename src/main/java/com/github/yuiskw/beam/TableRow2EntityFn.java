package com.github.yuiskw.beam;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.google.api.services.bigquery.model.TableRow;
import com.google.datastore.v1.*;
import com.google.protobuf.Timestamp;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;


public class TableRow2EntityFn extends DoFn<TableRow, Entity> {

  private String projectId;
  private String namespace;
  private LinkedHashMap<String, String> parents;
  private String kind;
  private String keyColumn;

  public TableRow2EntityFn(
      String projectId,
      String namespace,
      LinkedHashMap<String, String> parents,
      String kind,
      String keyColumn) {
    this.projectId = projectId;
    this.namespace = namespace;
    this.parents = parents;
    this.kind = kind;
    this.keyColumn = keyColumn;
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

  public Value convertToDatastoreValue(Object value) {
    Value v = null;
    if (value instanceof java.lang.Boolean) {
      v = Value.newBuilder().setBooleanValue(((Boolean) value).booleanValue())
          .setExcludeFromIndexes(true).build();
    }
    // INTEGER
    else if (value instanceof java.lang.Integer) {
      v = Value.newBuilder().setIntegerValue(((Integer) value).intValue())
          .setExcludeFromIndexes(true).build();
    }
    else if (value instanceof String && parseInteger((String) value) != null) {
      Integer integer = parseInteger((String) value);
      v = Value.newBuilder().setIntegerValue(integer.intValue())
          .setExcludeFromIndexes(true).build();
    }
    // LONG
    else if (value instanceof java.lang.Long) {
      v = Value.newBuilder().setIntegerValue((int) ((Long) value).longValue())
          .setExcludeFromIndexes(true).build();
    }
    // DOUBLE
    else if (value instanceof java.lang.Double) {
      v = Value.newBuilder().setDoubleValue(((Double) value).doubleValue())
          .setExcludeFromIndexes(true).build();
    }
    // TIMESTAMP
    else if (value instanceof org.joda.time.LocalDateTime) {
      Timestamp timestamp = toTimestamp(((LocalDateTime) value).toLocalDate().toDate());
      v = Value.newBuilder().setTimestampValue(timestamp)
          .setExcludeFromIndexes(true).build();
    }
    else if (value instanceof String && parseTimestamp((String) value) != null) {
      Date date = parseTimestamp((String) value);
      Timestamp timestamp = toTimestamp(date);
      v = Value.newBuilder().setTimestampValue(timestamp)
          .setExcludeFromIndexes(true).build();
    }
    // DATE
    else if (value instanceof org.joda.time.LocalDate) {
      Timestamp timestamp = toTimestamp(((LocalDate) value).toDate());
      v = Value.newBuilder().setTimestampValue(timestamp)
          .setExcludeFromIndexes(true).build();
    } else if (value instanceof String && parseDate((String) value) != null) {
      Date date = parseDate((String) value);
      Timestamp timestamp = toTimestamp(date);
      v = Value.newBuilder().setTimestampValue(timestamp)
          .setExcludeFromIndexes(true).build();
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
          .setExcludeFromIndexes(true).build();
    }
    // RECORD
    else if (value instanceof List) {
      ArrayValue.Builder arrayValueBuilder = ArrayValue.newBuilder();
      List<Object> records = (List<Object>) value;
      for (Object record : records) {
        Value subV = convertToDatastoreValue(record);
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
        Value subV = convertToDatastoreValue(struct.get(subKey));
        subEntityBuilder.putProperties(subKey, subV);
      }
      v = Value.newBuilder().setEntityValue(subEntityBuilder.build()).build();
    }
    return v;
  }

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
      Object value = entry.getValue();
      Value v = convertToDatastoreValue(value);
      if (v != null) {
        builder.putProperties(entry.getKey(), v);
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
      DateFormat sourceFormat = new SimpleDateFormat("yyyy-MM-dd");
      date = sourceFormat.parse(value);
    } catch (ParseException e) {
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
}

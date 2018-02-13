/**
 * Copyright (c) 2017 Yu Ishikawa.
 */
package com.github.yuiskw.beam;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * This class is used for a Dataflow job which write parsed Laplace logs to BigQuery.
 */
public class BigQuery2Datastore {

  /** command line options interface */
  public interface Optoins extends DataflowPipelineOptions {
    @Description("Input BigQuery dataset name")
    @Validation.Required
    String getInputBigQueryDataset();
    void setInputBigQueryDataset(String inputBigQueryDataset);

    @Description("Input BigQuery table name")
    @Validation.Required
    String getInputBigQueryTable();
    void setInputBigQueryTable(String inputBigQueryTable);

    @Description("Output Google Datastore namespace")
    @Validation.Required
    String getOutputDatastoreNamespace();
    void setOutputDatastoreNamespace(String outputDatastoreNamespace);

    @Description("Output Google Datastore kind")
    @Validation.Required
    String getOutputDatastoreKind();
    void setOutputDatastoreKind(String outputDatastoreKind);

    @Description("BigQuery column for Datastore key")
    @Validation.Required
    String getKeyColumn();
    void setKeyColumn(String keyColumn);

    @Description("Datastore parent path(s) (format: 'Parent1:p1,Parent2:p2')")
    String getParentPaths();
    void setparentPaths(String parentPaths);

    @Description("Indexed columns (format: 'column1,column2,column3')")
    String getIndexedColumns();
    void setIndexedColumns(String indexedColumns);
  }

  public static void main(String[] args) {
    Optoins options = getOptions(args);

    String projectId = options.getProject();
    String datasetId = options.getInputBigQueryDataset();
    String tableId = options.getInputBigQueryTable();
    String namespace = options.getOutputDatastoreNamespace();
    String kind = options.getOutputDatastoreKind();
    String keyColumn = options.getKeyColumn();
    LinkedHashMap<String, String> parents = parseParentPaths(options.getParentPaths());
    List<String> indexedColumns = parseIndexedColumns(options.getIndexedColumns());

    // Input
    TableReference tableRef = new TableReference().setDatasetId(datasetId).setTableId(tableId);
    BigQueryIO.Read reader = BigQueryIO.read().from(tableRef);

    // Output
    DatastoreV1.Write writer = DatastoreIO.v1().write().withProjectId(projectId);

    // Build and run pipeline
    TableRow2EntityFn fn =
        new TableRow2EntityFn(projectId, namespace, parents, kind, keyColumn, indexedColumns);
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(reader)
        .apply(ParDo.of(fn))
        .apply(writer);
    pipeline.run();
  }

  /**
   * Get command line options
   */
  public static Optoins getOptions(String[] args) {
    Optoins options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Optoins.class);
    return options;
  }

  /**
   * Get a parent path map
   *
   * e.g.) "Parent1:p1,Parent2:p2"
   */
  public static LinkedHashMap<String, String> parseParentPaths(String parentPaths) {
    LinkedHashMap<String, String> pathMap = new LinkedHashMap<String, String>();
    if (parentPaths != null) {
      // TODO validation
      for (String path : parentPaths.split(",")) {
        // trim
        String trimmed = path.replaceAll("(^\\s+|\\s+$)", "");

        // split with ":" and trim each element
        String[] elements = trimmed.split(":");
        String k = elements[0].replaceAll("(^\\s+|\\s+$)", "");
        String v = elements[1].replaceAll("(^\\s+|\\s+$)", "");
        pathMap.put(k, v);
      }
    }
    return pathMap;
  }

  /**
   * Get indexed column names
   *
   * @param indexedColumns a string separated by "," (i.e. "column1,column2,column3").
   * @return array of indexed column name.
   */
  public static List<String> parseIndexedColumns(String indexedColumns) {
    ArrayList<String> columns = new ArrayList<String>();
    if (indexedColumns != null) {
      for (String path : indexedColumns.split(",")) {
        // trim
        String column = path.replaceAll("(^\\s+|\\s+$)", "");
        columns.add(column);
      }
    }
    return columns;
  }
}

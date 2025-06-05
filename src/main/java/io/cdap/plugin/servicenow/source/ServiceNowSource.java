/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.servicenow.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import io.cdap.plugin.servicenow.util.ServiceNowTableInfo;
import io.cdap.plugin.servicenow.util.SourceQueryMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@link BatchSource} that reads data from multiple tables in ServiceNow.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(ServiceNowConstants.PLUGIN_NAME)
@Description("Reads from a table in ServiceNow. " +
  "Outputs one record for each row in each table, with the table name as a record field. " +
  "Also sets a pipeline argument for each table read, which contains the table schema. ")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = ServiceNowConstants.PLUGIN_NAME)})
public class ServiceNowSource extends BatchSource<NullWritable, StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceNowSource.class);

  private final ServiceNowSourceConfig conf;

  public ServiceNowSource(ServiceNowSourceConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);

    LOG.debug("Validate config during `configurePipeline` stage: {}", conf);
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();

    conf.validate(collector);
    // Since we have validated all the properties, throw an exception if there are any errors in the collector.
    // This is to avoid adding same validation errors again in getSchema method call
    collector.getOrThrowException();
    if (conf.shouldGetSchema() && conf.getQueryMode() == SourceQueryMode.TABLE) {
      List<ServiceNowTableInfo> tableInfo = ServiceNowInputFormat.fetchTableInfo(conf.getQueryMode(collector),
                                                                                 conf.getConnection(),
                                                                                 conf.getTableName(),
                                                                                 conf.getApplicationName(),
                                                                                 conf.getValueType());
      stageConfigurer.setOutputSchema(tableInfo.stream().findFirst().get().getSchema());
    } else if (conf.getQueryMode() == SourceQueryMode.REPORTING) {
      stageConfigurer.setOutputSchema(null);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    FailureCollector collector = context.getFailureCollector();
    conf.validate(collector);
    collector.getOrThrowException();

    SourceQueryMode mode = conf.getQueryMode(collector);

    Configuration hConf = new Configuration();
    Collection<ServiceNowTableInfo> tables = ServiceNowInputFormat.setInput(hConf, mode, conf);
    SettableArguments arguments = context.getArguments();
    for (ServiceNowTableInfo tableInfo : tables) {
      arguments.set(ServiceNowConstants.TABLE_PREFIX + tableInfo.getTableName(), tableInfo.getSchema().toString());
      recordLineage(context, tableInfo);
    }

    context.setInput(Input.of(conf.getReferenceName(),
                              new SourceInputFormatProvider(ServiceNowInputFormat.class, hConf)));
  }

  @Override
  public void transform(KeyValue<NullWritable, StructuredRecord> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(input.getValue());
  }

  private void recordLineage(BatchSourceContext context, ServiceNowTableInfo tableInfo) {
    String tableName = tableInfo.getTableName();
    String outputName = String.format("%s-%s", conf.getReferenceName(), tableName);
    Schema schema = tableInfo.getSchema();
    LineageRecorder lineageRecorder = new LineageRecorder(context, outputName);
    lineageRecorder.createExternalDataset(schema);
    List<Schema.Field> fields = Objects.requireNonNull(schema).getFields();
    if (fields != null && !fields.isEmpty()) {
      lineageRecorder.recordRead("Read",
                                 String.format("Read from '%s' ServiceNow table.", tableName),
                                 fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }
}

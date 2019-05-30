/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.salesforce.plugin.marketing;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
import org.apache.hadoop.io.NullWritable;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Writes to Salesforce Marketing Cloud Data Extensions.
 */
@Name("SalesforceDataExtension")
@Description("Inserts records into a Salesforce Marketing Cloud Data Extension.")
@Plugin(type = BatchSink.PLUGIN_TYPE)
public class MarketingCloudDataExtensionSink extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {
  private final MarketingCloudConf conf;

  public MarketingCloudDataExtensionSink(MarketingCloudConf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    conf.validate(pipelineConfigurer.getStageConfigurer().getInputSchema());
  }

  @Override
  public void prepareRun(BatchSinkContext batchSinkContext) {
    Schema inputSchema = batchSinkContext.getInputSchema();
    conf.validate(batchSinkContext.getInputSchema());

    LineageRecorder lineageRecorder = new LineageRecorder(batchSinkContext, conf.getReferenceName());
    if (inputSchema != null && inputSchema.getFields() != null) {
      lineageRecorder.recordWrite("Write", String.format("Wrote to Salesforce Marketing Cloud Data Extension %s",
                                                         conf.getDataExtension()),
                                  inputSchema.getFields().stream().map(Schema.Field::getName)
                                    .collect(Collectors.toList()));
    }

    batchSinkContext.addOutput(Output.of(conf.getReferenceName(), new OutputFormatProvider() {
      @Override
      public String getOutputFormatClassName() {
        return DataExtensionOutputFormat.class.getName();
      }

      @Override
      public Map<String, String> getOutputFormatConfiguration() {
        Map<String, String> outputConfig = new HashMap<>();
        outputConfig.put(DataExtensionOutputFormat.CLIENT_ID, conf.getClientId());
        outputConfig.put(DataExtensionOutputFormat.CLIENT_SECRET, conf.getClientSecret());
        outputConfig.put(DataExtensionOutputFormat.AUTH_ENDPOINT, conf.getAuthEndpoint());
        outputConfig.put(DataExtensionOutputFormat.SOAP_ENDPOINT, conf.getSoapEndpoint());
        outputConfig.put(DataExtensionOutputFormat.MAX_BATCH_SIZE, String.valueOf(conf.getMaxBatchSize()));
        outputConfig.put(DataExtensionOutputFormat.FAIL_ON_ERROR, String.valueOf(conf.shouldFailOnError()));
        outputConfig.put(DataExtensionOutputFormat.OPERATION, conf.getOperation().name());
        return outputConfig;
      }
    }));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, StructuredRecord>> emitter) {
    emitter.emit(new KeyValue<>(NullWritable.get(), input));
  }
}

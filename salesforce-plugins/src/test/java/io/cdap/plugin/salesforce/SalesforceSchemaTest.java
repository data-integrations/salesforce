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
package io.cdap.plugin.salesforce;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.plugin.salesforce.plugin.SalesforceConnectorInfo;
import io.cdap.plugin.salesforce.plugin.sink.batch.CSVRecord;
import io.cdap.plugin.salesforce.plugin.sink.batch.FileUploadSobject;
import io.cdap.plugin.salesforce.plugin.sink.batch.StructuredRecordToCSVRecordTransformer;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBatchMultiSource;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBatchSource;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceMultiSourceConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceSourceConfig;
import io.cdap.plugin.salesforce.plugin.source.streaming.SalesforceStreamingSource;
import io.cdap.plugin.salesforce.plugin.source.streaming.SalesforceStreamingSourceConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class SalesforceSchemaTest {
  @Test
  public void testSourceSchemaNotNullIfConnectionMacroAndImportManually() {
    Schema schema = Schema.recordOf("output",
        Schema.Field.of("Id",
            Schema.of(Schema.Type.STRING)));
    SalesforceSourceConfig mockConfig = Mockito.mock(SalesforceSourceConfig.class);
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null);
    SalesforceBatchSource source = new SalesforceBatchSource(mockConfig);
    SalesforceConnectorInfo mockConnection = Mockito.mock(SalesforceConnectorInfo.class);
    Mockito.when(mockConfig.getConnection()).thenReturn(mockConnection);
    Mockito.when(mockConfig.getConnection().canAttemptToEstablishConnection()).thenReturn(false);
    Mockito.when(mockConfig.getSchema()).thenReturn(schema);
    source.configurePipeline(mockPipelineConfigurer);
    Assert.assertNotNull(mockPipelineConfigurer.getOutputSchema());
    Assert.assertEquals(mockPipelineConfigurer.getOutputSchema(), schema);
  }

  @Test
  public void testStreamingSchemaNotNullIfConnectionMacroAndImportManually() {
    Schema schema = Schema.recordOf("output",
        Schema.Field.of("Id",
            Schema.of(Schema.Type.STRING)));
    SalesforceStreamingSourceConfig mockConfig = Mockito.mock(SalesforceStreamingSourceConfig.class);
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null);
    SalesforceStreamingSource source = new SalesforceStreamingSource(mockConfig);
    SalesforceConnectorInfo mockConnection = Mockito.mock(SalesforceConnectorInfo.class);
    Mockito.when(mockConfig.getConnection()).thenReturn(mockConnection);
    mockConfig.referenceName = "TestStreaming";
    Mockito.when(mockConfig.getConnection().canAttemptToEstablishConnection()).thenReturn(false);
    Mockito.when(mockConfig.getSchema()).thenReturn(schema);
    source.configurePipeline(mockPipelineConfigurer);
    Assert.assertNotNull(mockPipelineConfigurer.getOutputSchema());
    Assert.assertEquals(mockPipelineConfigurer.getOutputSchema(), schema);
  }

  @Test
  public void testMultiSourceSchemaNotNullIfConnectionMacroAndImportManually() {
    SalesforceMultiSourceConfig mockConfig = Mockito.mock(SalesforceMultiSourceConfig.class);
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null);
    SalesforceBatchMultiSource source = new SalesforceBatchMultiSource(mockConfig);
    mockConfig.referenceName = "TestStreaming";
    SalesforceConnectorInfo mockConnection = Mockito.mock(SalesforceConnectorInfo.class);
    Mockito.when(mockConfig.getConnection()).thenReturn(mockConnection);
    Mockito.when(mockConfig.getConnection().canAttemptToEstablishConnection()).thenReturn(false);
    source.configurePipeline(mockPipelineConfigurer);
    Assert.assertNull(mockPipelineConfigurer.getOutputSchema());
  }

  @Test
  public void testTransformWithAttachment() {
    Schema schema = Schema.recordOf("Schema",
        Schema.Field.of("Id", Schema.of(Schema.Type.INT)),
        Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
        Schema.Field.of("Body", Schema.of(Schema.Type.STRING)));
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    StructuredRecord record = builder.set("Id", 1)
        .set("Name", "attachment.pdf").set("Body", "base64-encoded value").build();
    FileUploadSobject sObjectName = FileUploadSobject.Attachment;
    // Call the transform method
    CSVRecord csvRecord = new StructuredRecordToCSVRecordTransformer().transform(record, sObjectName, 1);

    // Verify the results
    Assert.assertEquals(3, csvRecord.getColumnNames().size());
    Assert.assertEquals(3, csvRecord.getValues().size());
    Assert.assertEquals("Id", csvRecord.getColumnNames().get(0));
    // Body Field value will be replaced with record number concatenated with file name.
    Assert.assertEquals("#1_attachment.pdf", csvRecord.getValues().get(2));
  }

  @Test
  public void testTransformWithoutAttachment() {
    Schema schema = Schema.recordOf("Schema",
        Schema.Field.of("Id", Schema.of(Schema.Type.INT)),
        Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
        Schema.Field.of("Body", Schema.of(Schema.Type.STRING)));
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    StructuredRecord record = builder.set("Id", 1)
        .set("Name", "attachment.pdf").set("Body", "normal value").build();
    FileUploadSobject sObjectName = null;
    // Call the transform method
    CSVRecord csvRecord = new StructuredRecordToCSVRecordTransformer().transform(record, sObjectName, 1);

    // Verify the results
    Assert.assertEquals(3, csvRecord.getColumnNames().size());
    Assert.assertEquals(3, csvRecord.getValues().size());
    Assert.assertEquals("Id", csvRecord.getColumnNames().get(0));
    Assert.assertEquals("normal value", csvRecord.getValues().get(2));
  }
}

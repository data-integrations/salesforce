/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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
package io.cdap.plugin.salesforce.plugin.source.streaming;

/*import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;

import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(
  {SalesforceStreamingSourceUtil.class, Schema.class, SalesforceSchemaUtil.class, SObjectDescriptor.class})*/
public class SalesforceStreamingSourceUtilTest {

  /*@Test
  public void getStructuredRecordJavaDStreamTest() throws Exception {
    StreamingContext streamingContext = mock(StreamingContext.class);
    Schema schema = mock(Schema.class);
    mockStatic(SalesforceSchemaUtil.class);
    mockStatic(SObjectDescriptor.class);
    SObjectDescriptor sObjectDescriptor = mock(SObjectDescriptor.class);
    SalesforceStreamingSourceConfig config = mock(SalesforceStreamingSourceConfig.class);
    JavaStreamingContext javaStreamingContext = mock(JavaStreamingContext.class);
    JavaReceiverInputDStream javaReceiverInputDStream = mock(JavaReceiverInputDStream.class);
    AuthenticatorCredentials authenticatorCredentials = mock(AuthenticatorCredentials.class);
    StructuredRecord structuredRecord = mock(StructuredRecord.class);
    when(SalesforceSchemaUtil.getSchema(any(), any())).thenReturn(schema);
    when(config.getQuery()).thenReturn("pushTopic");
    when(SObjectDescriptor.fromQuery(anyString())).thenReturn(sObjectDescriptor);
    when(streamingContext.getSparkStreamingContext()).thenReturn(javaStreamingContext);
    when(config.getAuthenticatorCredentials()).thenReturn(authenticatorCredentials);
    when(config.getPushTopicName()).thenReturn("pushTopic");
    PowerMockito.spy(SalesforceStreamingSourceUtil.class);
    PowerMockito.doReturn(structuredRecord)
      .when(SalesforceStreamingSourceUtil.class, "getStructuredRecord", anyString(), any());
    when(javaStreamingContext.receiverStream(any())).thenReturn(javaReceiverInputDStream);
    try {
      SalesforceStreamingSourceUtil.getStructuredRecordJavaDStream(streamingContext, config);
    } catch (Exception e) {
      e.getMessage();
    }
  }

  @Ignore
  @Test
  public void getStructuredRecordTest() throws Exception {
    Schema schema = mock(Schema.class);
    schema = Schema.of(Schema.Type.STRING);
    Schema.Field s =
      Whitebox.invokeMethod(SalesforceStreamingSourceUtil.class, "getStructuredRecord", "", schema);
  }*/
}

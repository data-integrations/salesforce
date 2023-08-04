/*
 * Copyright © 2022 Cask Data, Inc.
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
package io.cdap.plugin.salesforce.connector;

import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import io.cdap.plugin.salesforce.plugin.SalesforceConnectorConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBatchSource;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceSourceConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceSourceConfigBuilder;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SObjectDescriptor.class, SalesforceSchemaUtil.class})
public class SalesforceConnectorTest {

  @Test
  public void testConnector() throws IOException, ConnectionException {
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setQuery("Select Name from Table")
      .setEnablePKChunk(true)
      .setChunkSize(SalesforceSourceConstants.MIN_PK_CHUNK_SIZE)
      .setReferenceName("Source").setSObjectName("object").build();
    SalesforceConnectorConfig connectorConfig = Mockito.mock(SalesforceConnectorConfig.class);
    MockFailureCollector collector = new MockFailureCollector();
    Mockito.when(connectorConfig.canAttemptToEstablishConnection()).thenReturn(false);
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    SalesforceConnector connector = new SalesforceConnector(connectorConfig);
    try {
      connector.test(context);
    } catch (Exception e) {
    }
    PowerMockito.mockStatic(SObjectDescriptor.class);
    SObjectDescriptor sObjectDescriptor = Mockito.mock(SObjectDescriptor.class);
    PowerMockito.when(SObjectDescriptor.fromName(Mockito.anyString(), Mockito.any(), Mockito.anySet())).
      thenReturn(sObjectDescriptor);
    PowerMockito.mockStatic(SalesforceSchemaUtil.class);
    PowerMockito.when(SalesforceSchemaUtil.getSchema(Mockito.any(), Mockito.any())).thenReturn(getPluginSchema());
    ConnectorSpec connectorSpec = connector.generateSpec(new MockConnectorContext(new MockConnectorConfigurer()),
                                                         ConnectorSpecRequest.builder().setPath
                                                             (config.getSObjectName())
                                                           .setConnection("${conn(connection-id)}").build());
    Set<PluginSpec> relatedPlugins = connectorSpec.getRelatedPlugins();
    Assert.assertEquals(2, relatedPlugins.size());
    PluginSpec pluginSpec = relatedPlugins.iterator().next();
    Assert.assertEquals(SalesforceBatchSource.NAME, pluginSpec.getName());
    Assert.assertEquals(BatchSource.PLUGIN_TYPE, pluginSpec.getType());
    Map<String, String> properties = pluginSpec.getProperties();
    Assert.assertEquals("true", properties.get(ConfigUtil.NAME_USE_CONNECTION));
    Assert.assertEquals("${conn(connection-id)}", properties.get(ConfigUtil.NAME_CONNECTION));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  private Schema getPluginSchema() throws IOException {
    String schemaString = "{\"type\":\"record\",\"name\":\"SalesforceData\",\"fields\":[{\"name\":" +
      "\"backgroundElementId\",\"type\":\"long\"},{\"name\":\"bgOrderPos\",\"type\":\"long\"},{\"name\":" +
      "\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"endDate\",\"type\":[{\"type\":\"long\"," +
      "\"logicalType\":\"timestamp-micros\"},\"null\"]},{\"name\":\"lastModifiedDate\",\"type\":" +
      "[{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"},\"null\"]},{\"name\":\"project\",\"type\":" +
      "\"string\"},{\"name\":\"startDate\",\"type\":[{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}," +
      "\"null\"]},{\"name\":\"userId\",\"type\":\"string\"}]}";

    return Schema.parseJson(schemaString);
  }

}

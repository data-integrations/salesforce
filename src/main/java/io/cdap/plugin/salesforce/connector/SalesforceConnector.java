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

import com.sforce.async.AsyncApiException;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseEntityPropertyValue;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.SalesforceConnectorConfig;
import io.cdap.plugin.salesforce.plugin.sink.batch.SalesforceBatchSink;
import io.cdap.plugin.salesforce.plugin.sink.batch.SalesforceSinkConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.MapToRecordTransformer;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBatchSource;
import io.cdap.plugin.salesforce.plugin.source.batch.SoapRecordToMapTransformer;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Salesforce Connector Plugin
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(SalesforceConstants.PLUGIN_NAME)
@Description("Connection to access data in Salesforce SObject.")
public class SalesforceConnector implements DirectConnector {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceConnector.class);
  private static final String ENTITY_TYPE_OBJECTS = "object";
  private static final String LABEL_NAME = "label";
  private final SalesforceConnectorConfig config;
  private StructuredRecord record;

  SalesforceConnector(SalesforceConnectorConfig config) {
    this.config = config;
  }

  @Override
  public void test(ConnectorContext connectorContext) throws ValidationException {
    FailureCollector collector = connectorContext.getFailureCollector();
    config.validate(collector);
  }

  @Override
  public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest browseRequest) throws IOException {
    AuthenticatorCredentials credentials = new AuthenticatorCredentials(config.getUsername(), config.getPassword(),
                                                                        config.getConsumerKey(),
                                                                        config.getConsumerSecret(),
                                                                        config.getLoginUrl());
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    int count = 0;
    try {
      PartnerConnection partnerConnection = SalesforceConnectionUtil.getPartnerConnection(credentials);
      DescribeGlobalResult dgr = partnerConnection.describeGlobal();
      // Loop through the array echoing the object names to the console
      for (int i = 0; i < dgr.getSobjects().length; i++) {
        String name = dgr.getSobjects()[i].getName();
        String label = dgr.getSobjects()[i].getLabel();
        BrowseEntity.Builder entity = (BrowseEntity.builder(name, name, ENTITY_TYPE_OBJECTS).
          canBrowse(false).canSample(true));
        entity.addProperty(LABEL_NAME, BrowseEntityPropertyValue.builder(label, BrowseEntityPropertyValue.
          PropertyType.STRING).build());
        browseDetailBuilder.addEntity(entity.build());
        count++;
      }
    } catch (ConnectionException e) {
      throw  new IOException("Unable to create the connection.", e);
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  @Override
  public ConnectorSpec generateSpec(ConnectorContext connectorContext, ConnectorSpecRequest connectorSpecRequest)
    throws IOException {
    ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
    Map<String, String> properties = new HashMap<>();
    properties.put(io.cdap.plugin.common.ConfigUtil.NAME_USE_CONNECTION, "true");
    properties.put(ConfigUtil.NAME_CONNECTION, connectorSpecRequest.getConnectionWithMacro());
    String tableName = connectorSpecRequest.getPath();
    if (tableName != null) {
      properties.put(SalesforceSourceConstants.PROPERTY_SOBJECT_NAME, tableName);
      properties.put(SalesforceSinkConfig.PROPERTY_SOBJECT, tableName);
    }
    AuthenticatorCredentials authenticatorCredentials = config.getAuthenticatorCredentials();
    try {
      SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromName(tableName, authenticatorCredentials);
      Schema schema = SalesforceSchemaUtil.getSchema(authenticatorCredentials, sObjectDescriptor);
      specBuilder.setSchema(schema);
    } catch (ConnectionException e) {
      throw new IOException("Unable to generate Schema", e);
    }
    return specBuilder.addRelatedPlugin(new PluginSpec(SalesforceBatchSource.NAME, BatchSource.PLUGIN_TYPE,
                                                       properties)).
      addRelatedPlugin(new PluginSpec(SalesforceBatchSink.PLUGIN_NAME, BatchSink.PLUGIN_TYPE, properties)).build();
  }

  @Override
  public List<StructuredRecord> sample(ConnectorContext connectorContext, SampleRequest sampleRequest)
    throws IOException {
    String object = sampleRequest.getPath();
    if (object == null) {
      throw new IllegalArgumentException("Path should contain object");
    }
    try {
      return listObjectDetails(object, sampleRequest.getLimit());
    } catch (AsyncApiException | ConnectionException e) {
      throw new IOException("unable to fetch records", e);
    }
  }

  private List<StructuredRecord> listObjectDetails(String object, int limit) throws AsyncApiException,
    ConnectionException {
    List<StructuredRecord> samples = new ArrayList<>();
    AuthenticatorCredentials credentials = new AuthenticatorCredentials(config.getUsername(), config.getPassword(),
                                                                        config.getConsumerKey(),
                                                                        config.getConsumerSecret(),
                                                                        config.getLoginUrl());
    String fields = getObjectFields(object);
    String query = String.format("SELECT %s FROM %s LIMIT %d", fields, object, limit);
    SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(query);
    SoapRecordToMapTransformer soapRecordToMapTransformer = new SoapRecordToMapTransformer();
    PartnerConnection partnerConnection = SalesforceConnectionUtil.getPartnerConnection(credentials);
    QueryResult queryResult = partnerConnection.query(query);
    SObject[] sObjects = queryResult.getRecords();
    Schema schema = SalesforceSchemaUtil.getSchema(credentials, sObjectDescriptor);
    MapToRecordTransformer transformer = new MapToRecordTransformer();
    for (int i = 0; i < sObjects.length; i++) {
      record = transformer.transform(schema, soapRecordToMapTransformer.transformToMap(sObjects[i], sObjectDescriptor));
      samples.add(record);
    }

    return samples;
  }

  private String getObjectFields(String object) throws ConnectionException {
    SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromName(object, config.getAuthenticatorCredentials(),
                                                                     SalesforceSchemaUtil.COMPOUND_FIELDS);
    List<String> actualFields = sObjectDescriptor.getFieldsNames();
    String result = String.join(",", actualFields);
    return result;
  }

}

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
package io.cdap.plugin.servicenow.connector;

import com.google.gson.Gson;
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
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.servicenow.apiclient.ServiceNowAPIException;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIRequestBuilder;
import io.cdap.plugin.servicenow.restapi.RestAPIResponse;
import io.cdap.plugin.servicenow.source.ServiceNowInputFormat;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import io.cdap.plugin.servicenow.util.ServiceNowTableInfo;
import io.cdap.plugin.servicenow.util.SourceQueryMode;
import io.cdap.plugin.servicenow.util.SourceValueType;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;


/**
 * ServiceNow Connector Plugin
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(ServiceNowConstants.PLUGIN_NAME)
@Description("Connection to access data in Servicenow tables.")
public class ServiceNowConnector implements DirectConnector {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceNowConnector.class);
  private static final String OBJECT_TABLE_LIST = "sys_db_object";
  private static final String ENTITY_TYPE_TABLE = "table";
  private static final String LABEL_NAME = "label";
  private static final Gson GSON = new Gson();
  private final ServiceNowConnectorConfig config;

  public ServiceNowConnector(ServiceNowConnectorConfig config) {
    this.config = config;
  }

  @Override
  public void test(ConnectorContext connectorContext) throws ValidationException {
    FailureCollector collector = connectorContext.getFailureCollector();
    config.validateCredentialsFields(collector);
    config.validateConnection(collector);
  }

  @Override
  public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest browseRequest) throws IOException {
    ServiceNowTableAPIClientImpl serviceNowTableAPIClient = new ServiceNowTableAPIClientImpl(config);
    try {
      String accessToken = serviceNowTableAPIClient.getAccessToken();
      return browse(connectorContext, accessToken);
    } catch (ServiceNowAPIException e) {
      throw new IOException(e);
    }
  }

  /**
   * Browse Details for the given AccessToken.
   */
  public BrowseDetail browse(ConnectorContext connectorContext,
                             String accessToken) throws ServiceNowAPIException {
    int count = 0;
    FailureCollector collector = connectorContext.getFailureCollector();
    config.validateCredentialsFields(collector);
    collector.getOrThrowException();
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    Table[] table = listTables(accessToken).getResult();
    for (int i = 0; i < table.length; i++) {
      String name = table[i].getName();
      String label = table[i].getLabel();
      BrowseEntity.Builder entity = (BrowseEntity.builder(name, name, ENTITY_TYPE_TABLE).
        canBrowse(false).canSample(true));
      entity.addProperty(LABEL_NAME, BrowseEntityPropertyValue.builder(label, BrowseEntityPropertyValue.
        PropertyType.STRING).build());
      browseDetailBuilder.addEntity(entity.build());
      count++;
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  /**
   * @return the list of tables.
   */
  private TableList listTables(String accessToken) throws ServiceNowAPIException {
    ServiceNowTableAPIRequestBuilder requestBuilder = new ServiceNowTableAPIRequestBuilder(
      config.getRestApiEndpoint(), OBJECT_TABLE_LIST, false);
    requestBuilder.setAuthHeader(accessToken);
    requestBuilder.setAcceptHeader(MediaType.APPLICATION_JSON);
    requestBuilder.setContentTypeHeader(MediaType.APPLICATION_JSON);
    ServiceNowTableAPIClientImpl serviceNowTableAPIClient = new ServiceNowTableAPIClientImpl(config);
    RestAPIResponse apiResponse =
        serviceNowTableAPIClient.executeGetWithRetries(requestBuilder.build());
    return GSON.fromJson(apiResponse.getResponseBody(), TableList.class);
  }

  public ConnectorSpec generateSpec(ConnectorContext connectorContext, ConnectorSpecRequest connectorSpecRequest) {
    ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
    Map<String, String> properties = new HashMap<>();
    properties.put(io.cdap.plugin.common.ConfigUtil.NAME_USE_CONNECTION, "true");
    properties.put(ConfigUtil.NAME_CONNECTION, connectorSpecRequest.getConnectionWithMacro());
    String tableName = connectorSpecRequest.getPath();
    if (tableName != null) {
      properties.put(ServiceNowConstants.PROPERTY_TABLE_NAME, tableName);
      properties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(tableName));
    }
    Schema schema = getSchema(tableName);
    if (schema != null) {
      specBuilder.setSchema(schema);
    }
    return specBuilder.addRelatedPlugin(new PluginSpec(ServiceNowConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE,
                                                       properties))
      .addRelatedPlugin(new PluginSpec(ServiceNowConstants.PLUGIN_NAME, BatchSink.PLUGIN_TYPE, properties)).build();
  }

  @Override
  public List<StructuredRecord> sample(ConnectorContext connectorContext, SampleRequest sampleRequest)
    throws IOException {
    String table = sampleRequest.getPath();
    if (table == null) {
      throw new IllegalArgumentException("Path should contain table name.");
    }
    try {
      return getTableData(table, sampleRequest.getLimit());
    } catch (OAuthProblemException | OAuthSystemException | ServiceNowAPIException e) {
      throw new IOException("Unable to fetch the data.", e);
    }
  }

  private List<StructuredRecord> getTableData(String tableName, int limit)
      throws OAuthProblemException, OAuthSystemException, ServiceNowAPIException {
    ServiceNowTableAPIRequestBuilder requestBuilder = new ServiceNowTableAPIRequestBuilder(
      config.getRestApiEndpoint(), tableName, false)
      .setExcludeReferenceLink(true)
      .setDisplayValue(SourceValueType.SHOW_DISPLAY_VALUE)
      .setLimit(limit);
    ServiceNowTableAPIClientImpl serviceNowTableAPIClient = new ServiceNowTableAPIClientImpl(config);
    String accessToken = serviceNowTableAPIClient.getAccessToken();
    requestBuilder.setAuthHeader(accessToken);
    requestBuilder.setResponseHeaders(ServiceNowConstants.HEADER_NAME_TOTAL_COUNT);
    RestAPIResponse apiResponse = serviceNowTableAPIClient.executeGetWithRetries(requestBuilder.build());
    List<Map<String, String>> result = serviceNowTableAPIClient.parseResponseToResultListOfMap
      (apiResponse.getResponseBody());
    List<StructuredRecord> recordList = new ArrayList<>();
    Schema schema = getSchema(tableName);
    if (schema != null) {
      List<Schema.Field> tableFields = schema.getFields();
      for (int i = 0; i < result.size(); i++) {
        StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
        for (Schema.Field field : tableFields) {
          String fieldName = field.getName();
          ServiceNowRecordConverter.convertToValue(fieldName, field.getSchema(), result.get(i), recordBuilder);
        }
        StructuredRecord structuredRecord = recordBuilder.build();
        recordList.add(structuredRecord);
      }
    }
    return recordList;

  }

  @Nullable
  private Schema getSchema(String tableName) {
    SourceQueryMode mode = SourceQueryMode.TABLE;
    // Use display type schema as connector shows a limited number of values
    // and display value type provides easy to read values
    List<ServiceNowTableInfo> tableInfo = ServiceNowInputFormat.fetchTableInfo(
      mode,
      config,
      tableName,
      null,
      SourceValueType.SHOW_DISPLAY_VALUE);
    Schema schema = tableInfo.stream().findFirst().isPresent() ? tableInfo.stream().findFirst().get().getSchema() :
      null;
    return schema;
  }
}

/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.salesforce.connectorservice;

import com.google.cloud.connector.api.AssetName;
import com.google.cloud.connector.api.Connector;
import com.google.cloud.connector.api.ConnectorContext;
import com.google.cloud.connector.api.annotation.DataSource;
import com.google.cloud.connector.api.browse.BrowseEntityListBuilder;
import com.google.cloud.connector.api.data.RecordReader;
import com.google.cloud.connector.api.schema.FieldBuilder;
import com.google.cloud.connector.api.schema.SchemaBuilder;
import com.google.common.base.Preconditions;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.connector.SalesforceConnector;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.SalesforceConnectorConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBatchSource;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceSoapRecordReader;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceSourceConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.SoapRecordToMapTransformer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

import static com.google.cloud.bigquery.federation.v1alpha1.DataSource.Capability.SUPPORTS_SYNCHRONOUS_QUERIES;


/** Connector to test connector loading. */
public class ConnectorServiceSalesforce implements Connector {

  private final ConnectorServiceSalesforceConfig config;

  @DataSource(capabilities = {SUPPORTS_SYNCHRONOUS_QUERIES})
  public ConnectorServiceSalesforce(ConnectorServiceSalesforceConfig config) {
    this.config = config;
  }

  @Override
  public String toString() {
    return config.toString();
  }

  @Override
  public void resolveSchema(AssetName assetName, ConnectorContext context) {
    validateAssetName(assetName);

    System.out.println("Resolve schema for assertName " + assetName.name());

    String sObjectName = getsObjectName(assetName);

    SalesforceSourceConfig sourceConfig =
        SalesforceSourceConfig.fromConnectorServiceConfig(sObjectName, config);
    SalesforceConnectorConfig connectorConfig =
        SalesforceConnectorConfig.fromConnectorServiceConfig(config);

    FailureCollector collector = new SimpleFailureCollector();
    OAuthInfo oAuthInfo = SalesforceConnectionUtil.getOAuthInfo(connectorConfig, collector);
    sourceConfig.validate(collector, oAuthInfo);

    Schema schema = SalesforceBatchSource.getSchema(sourceConfig, oAuthInfo);

    SchemaBuilder schemaBuilder = context.getSchemaBuilder();

    convertSchemaToConnectorService(schema, schemaBuilder);
  }

  @Override
  public void browse(AssetName assetName, ConnectorContext context) throws Exception {
    SalesforceConnectorConfig connectorConfig =
        new io.cdap.plugin.salesforce.plugin.SalesforceConnectorConfig(
            config.consumerKey(),
            config.consumerSecret(),
            config.username(),
            config.password(),
            config.loginUrl(),
            config.securityToken(),
            config.connectTimeout(),
            null,
            null);

    SalesforceConnector salesforceConnector = new SalesforceConnector(connectorConfig);
    BrowseDetail browseDetail = salesforceConnector.browse(connectorConfig);

    BrowseEntityListBuilder builder = context.getBrowseEntityListBuilder();
    for (BrowseEntity e : browseDetail.getEntities()) {
      builder.add(e.getName(), e.canBrowse());
    }
  }

  @Override
  public RecordReader executeQuery(AssetName assetName, ConnectorContext context) throws Exception {

    validateAssetName(assetName);

    System.out.println("Execute query for asset name " + assetName.name());

    String sObjectName = getsObjectName(assetName);

    SalesforceSourceConfig sourceConfig =
        SalesforceSourceConfig.fromConnectorServiceConfig(sObjectName, config);
    SalesforceConnectorConfig connectorConfig =
        SalesforceConnectorConfig.fromConnectorServiceConfig(config);

    FailureCollector collector = new SimpleFailureCollector();
    OAuthInfo oAuthInfo = SalesforceConnectionUtil.getOAuthInfo(connectorConfig, collector);
    sourceConfig.validate(collector, oAuthInfo);

    Schema schema = SalesforceBatchSource.getSchema(sourceConfig, oAuthInfo);
    // TODO(wyzhang): pass in the application time?
    String query = sourceConfig.getQuery(System.currentTimeMillis(), oAuthInfo);

    SalesforceSoapRecordReader recordReader =
        new SalesforceSoapRecordReader(schema, query, new SoapRecordToMapTransformer());

    AuthenticatorCredentials credentials =
        new AuthenticatorCredentials(oAuthInfo, config.connectTimeout(), null);

    recordReader.initialize(credentials);

    return (RecordReader) new ConnectorServiceRecordReader(recordReader);
  }

  private String getSObjectName(AssetName assetName) {
    Preconditions.checkArgument(
        assetName.components().size() == 2,
        String.format(
            "Salesforce asset name should have 2 components "
                + "datasources/{}/sobjects/{}, but got '%s'",
            assetName));
    return assetName.components().get(1).resourceId();
  }

  private void convertSchemaToConnectorService(Schema schema, SchemaBuilder builder) {
    System.out.println("wyzhang: cdap schema " + schema);
    builder.name("salesforce-schema");
    for (Schema.Field f : schema.getFields()) {
      FieldBuilder fieldBuilder = builder.field().name(f.getName());
      fromCdapType(f.getSchema(), fieldBuilder);
    }
  }

  private String getsObjectName(AssetName assetName) {
    String sObjectName = assetName.components().get(1).resourceId();
    return sObjectName;
  }

  private void validateAssetName(AssetName assetName) {
    Preconditions.checkArgument(
        assetName.components().size() == 2,
        "Asset name should be datasources/salesforce/sobjects/{}");
    Preconditions.checkArgument(
        assetName.components().get(1).resourceId() != null
            && !assetName.components().get(1).resourceId().isEmpty(),
        "Asset name {} in datasources/salesforce/sobjects/{} should not be empty");
  }

  private void fromCdapType(Schema schema, FieldBuilder fieldBuilder) {
    switch (schema.getType()) {
      case NULL:
        throw new UnsupportedOperationException("Salesforce null type is unsupported");
      case BOOLEAN:
        fieldBuilder.typeBoolean();
        break;
      case INT:
      case LONG:
        fieldBuilder.typeInteger();
        break;
      case FLOAT:
      case DOUBLE:
        fieldBuilder.typeFloat();
        break;
      case BYTES:
        fieldBuilder.typeBytes();
        break;
      case STRING:
        fieldBuilder.typeString();
        break;
      case ENUM:
        throw new UnsupportedOperationException("Salesforce enum type is unsupported");
      case ARRAY:
        throw new UnsupportedOperationException("Salesforce array type is unsupported");
      case MAP:
        throw new UnsupportedOperationException("Salesforce map type is unsupported");
      case RECORD:
        throw new UnsupportedOperationException("Salesforce record type is unsupported");
      case UNION:
        boolean set = false;
        for (Schema unionSchema : schema.getUnionSchemas()) {
          if (unionSchema.getType() == Schema.Type.NULL) {
            continue;
          }
          if (!set) {
            fromCdapType(unionSchema, fieldBuilder);
            set = true;
          } else {
            throw new UnsupportedOperationException(
                "Salesforce union type with multiple non-null is unsupported");
          }
        }
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Salesforce type '%s' is unsupported", schema.getType()));
    }
  }

  private class SimpleFailureCollector implements io.cdap.cdap.etl.api.FailureCollector {
    private final List<ValidationFailure> failures;

    SimpleFailureCollector() {
      failures = new ArrayList<>();
    }

    @Override
    public ValidationFailure addFailure(String message, @Nullable String correctiveAction) {
      ValidationFailure failure =
          new ValidationFailure("", correctiveAction, "", Collections.emptyMap());
      failures.add(failure);
      return failure;
    }

    @Override
    public ValidationException getOrThrowException() throws ValidationException {
      if (failures.isEmpty()) {
        return new ValidationException(failures);
      }
      throw new ValidationException(failures);
    }

    public List<ValidationFailure> getValidationFailures() {
      return failures;
    }
  }
}

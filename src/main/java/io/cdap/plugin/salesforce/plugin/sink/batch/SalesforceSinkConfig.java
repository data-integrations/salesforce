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
package io.cdap.plugin.salesforce.plugin.sink.batch;

import com.google.common.base.Strings;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.OperationEnum;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.salesforce.InvalidConfigException;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SObjectsDescribeResult;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.SalesforceConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Provides the configurations for {@link SalesforceBatchSink} plugin.
 */
public class SalesforceSinkConfig extends ReferencePluginConfig {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceSinkConfig.class);
  public static final String PROPERTY_ERROR_HANDLING = "errorHandling";
  public static final String PROPERTY_MAX_BYTES_PER_BATCH = "maxBytesPerBatch";
  public static final String PROPERTY_MAX_RECORDS_PER_BATCH = "maxRecordsPerBatch";
  public static final String PROPERTY_SOBJECT = "sObject";
  public static final String PROPERTY_OPERATION = "operation";
  public static final String PROPERTY_EXTERNAL_ID_FIELD = "externalIdField";
  public static final String PROPERTY_CONCURRENCY_MODE = "concurrencyMode";

  private static final String SALESFORCE_ID_FIELD = "Id";

  /**
   * According to "Bulk API Limitations" batch cannot be larger than 10 megabytes.
   */
  private static final long MAX_BYTES_PER_BATCH_LIMIT = 10_000_000;
  /**
   * According to "Bulk API Limitations" batch cannot contain more than 10,000 records.
   */
  private static final long MAX_RECORDS_PER_BATCH_LIMIT = 10_000;

  @Name(PROPERTY_SOBJECT)
  @Description("Salesforce object name to insert records into.")
  @Macro
  private String sObject;

  @Name(PROPERTY_OPERATION)
  @Description("Operation used for sinking data into Salesforce.\n" +
    "Insert - adds records.\n" +
    "Upsert - upserts the records. Salesforce will decide if sObjects " +
    "are the same using external id field.\n" +
    "Update - updates existing records based on Id field.")
  @Macro
  private String operation;

  @Name(PROPERTY_EXTERNAL_ID_FIELD)
  @Description("External id field name. It is used only if operation is upsert.\n" +
    "The field specified can be either 'Id' or any customly created field, which has external id attribute set.")
  @Nullable
  @Macro
  private String externalIdField;

  @Name(PROPERTY_CONCURRENCY_MODE)
  @Description("The concurrency mode for the bulk job. The valid values are: \n" +
    "Parallel - Process batches in parallel mode. This is the default value.\n" +
    "Serial - Process batches in serial mode. Processing in parallel can cause database contention. " +
    "When this is severe, the job can fail. If you’re experiencing this issue, submit the job with serial " +
    "concurrency mode. This mode guarantees that batches are processed one at a time, but can significantly " +
    "increase the processing time.")
  @Macro
  @Nullable
  protected String concurrencyMode;

  @Name(PROPERTY_MAX_BYTES_PER_BATCH)
  @Description("Maximum size in bytes of a batch of records when writing to Salesforce. " +
    "This value cannot be greater than 10,000,000.")
  @Macro
  private String maxBytesPerBatch;

  @Name(PROPERTY_MAX_RECORDS_PER_BATCH)
  @Description("Maximum number of records to include in a batch when writing to Salesforce." +
    "This value cannot be greater than 10,000.")
  @Macro
  private String maxRecordsPerBatch;

  @Name(PROPERTY_ERROR_HANDLING)
  @Description("Strategy used to handle erroneous records.\n" +
    "Skip on error - Ignores erroneous records.\n" +
    "Stop on error - Fails pipeline due to erroneous record.")
  @Macro
  private String errorHandling;

  @Name(ConfigUtil.NAME_USE_CONNECTION)
  @Nullable
  @Description("Whether to use an existing connection.")
  private Boolean useConnection;

  @Name(ConfigUtil.NAME_CONNECTION)
  @Macro
  @Nullable
  @Description("The existing connection to use.")
  private SalesforceConnectorConfig connection;

  public SalesforceSinkConfig(String referenceName,
                              @Nullable String clientId,
                              @Nullable String clientSecret,
                              @Nullable String username,
                              @Nullable String password,
                              @Nullable String loginUrl,
                              @Nullable Integer connectTimeout,
                              String sObject,
                              String operation, String externalIdField, String concurrencyMode,
                              String maxBytesPerBatch, String maxRecordsPerBatch,
                              String errorHandling,
                              @Nullable String securityToken,
                              @Nullable OAuthInfo oAuthInfo,
                              @Nullable String proxyUrl) {
    super(referenceName);
    connection = new SalesforceConnectorConfig(clientId, clientSecret, username, password, loginUrl,
                                               securityToken, connectTimeout, oAuthInfo, proxyUrl);
    this.sObject = sObject;
    this.operation = operation;
    this.externalIdField = externalIdField;
    this.concurrencyMode = concurrencyMode;
    this.maxBytesPerBatch = maxBytesPerBatch;
    this.maxRecordsPerBatch = maxRecordsPerBatch;
    this.errorHandling = errorHandling;
  }

  private static final String DEFAULT_LOGIN_URL = "https://login.salesforce.com/services/oauth2/token";

  @Nullable
  public SalesforceConnectorConfig getConnection() {
    return connection;
  }

  public String getSObject() {
    return sObject;
  }

  public String getOperation() {
    return operation;
  }

  public OperationEnum getOperationEnum() {
    try {
      return OperationEnum.valueOf(operation.toLowerCase());
    } catch (IllegalArgumentException ex) {
      throw new InvalidConfigException("Unsupported value for operation: " + operation,
                                       SalesforceSinkConfig.PROPERTY_OPERATION);
    }
  }

  public String getExternalIdField() {
    return externalIdField;
  }

  public String getConcurrencyMode() {
    return concurrencyMode;
  }

  public ConcurrencyMode getConcurrencyModeEnum() {
    try {
      return ConcurrencyMode.valueOf(concurrencyMode);
    } catch (Exception ex) {
      LOG.info("Value received was {}. Default concurrency mode i.e. Parallel is used instead.", concurrencyMode);
      return ConcurrencyMode.Parallel;
    }
  }

  public Long getMaxBytesPerBatch() {
    try {
      return Long.parseLong(maxBytesPerBatch);
    } catch (NumberFormatException ex) {
      throw new InvalidConfigException("Unsupported value for maxBytesPerBatch: " + maxBytesPerBatch,
                                       SalesforceSinkConfig.PROPERTY_MAX_BYTES_PER_BATCH);
    }
  }

  public Long getMaxRecordsPerBatch() {
    try {
      return Long.parseLong(maxRecordsPerBatch);
    } catch (NumberFormatException ex) {
      throw new InvalidConfigException("Unsupported value for maxRecordsPerBatch: " + maxRecordsPerBatch,
                                       SalesforceSinkConfig.PROPERTY_MAX_RECORDS_PER_BATCH);
    }
  }

  public ErrorHandling getErrorHandling() {
    return ErrorHandling.fromValue(errorHandling)
      .orElseThrow(() -> new InvalidConfigException("Unsupported error handling value: " + errorHandling,
                                                    SalesforceSinkConfig.PROPERTY_ERROR_HANDLING));
  }

  public String getReferenceNameOrNormalizedFQN(String orgId, String sObject) {
    return Strings.isNullOrEmpty(referenceName)
      ? ReferenceNames.normalizeFqn(getFQN(orgId, sObject))
      : referenceName;
  }

  /**
   * Get fully-qualified name (FQN) for a Salesforce object (FQN format: salesforce://prod/orgId.mySobject).
   *
   * @return String fqn
   */
  public String getFQN(String orgId, String sObject) {
    String firstFQNPart = connection.getLoginUrl().equals(DEFAULT_LOGIN_URL) ? "prod" : "sandbox";
    return String.format("salesforce://%s/%s.%s", firstFQNPart, orgId, sObject);
  }

  public String getOrgId(OAuthInfo oAuthInfo) throws ConnectionException {
    AuthenticatorCredentials credentials = new AuthenticatorCredentials(oAuthInfo,
                                                                        this.getConnection().getConnectTimeout(),
                                                                        this.connection.getProxyUrl());
    PartnerConnection partnerConnection = SalesforceConnectionUtil.getPartnerConnection
      (credentials);
    return partnerConnection.getUserInfo().getOrganizationId();
  }

  public void validate(Schema schema, FailureCollector collector, OAuthInfo oAuthInfo) {
    if (connection != null) {
      connection.validate(collector, oAuthInfo);
    }
    validateSinkProperties(collector);
    validateSchema(schema, collector, oAuthInfo);
  }

  public void validateSinkProperties(FailureCollector collector) {
    if (!containsMacro(PROPERTY_ERROR_HANDLING)) {
      // triggering getter will also trigger value validity check
      try {
        getErrorHandling();
      } catch (InvalidConfigException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(PROPERTY_ERROR_HANDLING);
      }
    }

    if (!containsMacro(PROPERTY_OPERATION)) {
      // triggering getter will also trigger value validity check
      try {
        getOperationEnum();
      } catch (InvalidConfigException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(PROPERTY_OPERATION);
      }
    }

    if (!containsMacro(PROPERTY_CONCURRENCY_MODE)) {
      // triggering getter will also trigger value validity check
      try {
        getConcurrencyModeEnum();
      } catch (InvalidConfigException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(PROPERTY_CONCURRENCY_MODE);
      }
    }

    if (!containsMacro(PROPERTY_MAX_BYTES_PER_BATCH)) {
      long maxBytesPerBatch = getMaxBytesPerBatch();

      if (maxBytesPerBatch <= 0 || maxBytesPerBatch > MAX_BYTES_PER_BATCH_LIMIT) {
        String errorMessage = String.format(
          "Unsupported value for maxBytesPerBatch: %d. Value should be between 1 and %d",
          maxBytesPerBatch, MAX_BYTES_PER_BATCH_LIMIT);
        collector.addFailure(errorMessage, null).withConfigProperty(PROPERTY_MAX_BYTES_PER_BATCH);
      }
    }

    if (!containsMacro(PROPERTY_MAX_RECORDS_PER_BATCH)) {
      long maxRecordsPerBatch = getMaxRecordsPerBatch();

      if (maxRecordsPerBatch <= 0 || maxRecordsPerBatch > MAX_RECORDS_PER_BATCH_LIMIT) {
        String errorMessage = String.format(
          "Unsupported value for maxRecordsPerBatch: %d. Value should be between 1 and %d",
          maxRecordsPerBatch, MAX_RECORDS_PER_BATCH_LIMIT);
        collector.addFailure(errorMessage, null).withConfigProperty(PROPERTY_MAX_RECORDS_PER_BATCH);
      }
    }
    collector.getOrThrowException();
  }

  private void validateSchema(Schema schema, FailureCollector collector, OAuthInfo oAuthInfo) {
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      collector.addFailure("Sink schema must contain at least one field", null);
      throw collector.getOrThrowException();
    }
    if (connection != null) {
      if (!connection.canAttemptToEstablishConnection() || containsMacro(PROPERTY_SOBJECT)
        || containsMacro(PROPERTY_OPERATION) || containsMacro(PROPERTY_EXTERNAL_ID_FIELD)) {
        return;
      }
    }
    SObjectsDescribeResult describeResult = getSObjectDescribeResult(collector, oAuthInfo);
    Set<String> creatableSObjectFields = getCreatableSObjectFields(describeResult);

    Set<String> inputFields = schema.getFields()
      .stream()
      .map(Schema.Field::getName)
      .collect(Collectors.toSet());

    OperationEnum operation = getOperationEnum();

    String externalIdFieldName = null;
    switch (operation) {
      case insert:
        break;
      case upsert:
        externalIdFieldName = getExternalIdField();
        break;
      case update:
        externalIdFieldName = SALESFORCE_ID_FIELD;
        break;
      default:
        collector.addFailure("Unsupported value for operation: " + operation, null)
          .withConfigProperty(PROPERTY_OPERATION);
    }

    if (operation == OperationEnum.upsert) {
      Field externalIdField = describeResult.getField(sObject, externalIdFieldName);
      if (externalIdField == null) {
        collector.addFailure(
            String.format("SObject '%s' does not contain external id field '%s'", sObject, externalIdFieldName), null)
          .withConfigProperty(SalesforceSinkConfig.PROPERTY_EXTERNAL_ID_FIELD);
      } else if (!externalIdField.isExternalId() && !externalIdField.getName().equals(SALESFORCE_ID_FIELD)) {
        collector.addFailure(
            String.format("Field '%s' is not configured as external id in Salesforce", externalIdFieldName), null)
          .withConfigProperty(SalesforceSinkConfig.PROPERTY_EXTERNAL_ID_FIELD);
      }
    } else if (operation == OperationEnum.insert || operation == OperationEnum.update) {
      if (!Strings.isNullOrEmpty(getExternalIdField())) {
        collector.addFailure(String.format("External id field must not be set for operation='%s'", operation), null)
          .withConfigProperty(SalesforceSinkConfig.PROPERTY_EXTERNAL_ID_FIELD);
      }
    }

    if (externalIdFieldName != null && !inputFields.remove(externalIdFieldName)) {
      collector.addFailure(String.format("Schema must contain external id field '%s'", externalIdFieldName), null)
        .withConfigProperty(SalesforceSinkConfig.PROPERTY_EXTERNAL_ID_FIELD);
    }
    inputFields.removeAll(creatableSObjectFields);

    if (!inputFields.isEmpty()) {
      for (String inputField : inputFields) {
        collector.addFailure(
            String.format("Field '%s' is not present or not creatable in target Salesforce sObject.", inputField), null)
          .withInputSchemaField(inputField);
      }
    }
  }

  private Set<String> getCreatableSObjectFields(SObjectsDescribeResult describeResult) {
    Set<String> creatableSObjectFields = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    for (Field field : describeResult.getFields()) {
      if (field.isCreateable()) {
        creatableSObjectFields.add(field.getName());
      }
    }
    return creatableSObjectFields;
  }

  private SObjectsDescribeResult getSObjectDescribeResult(FailureCollector collector, OAuthInfo oAuthInfo) {
    AuthenticatorCredentials credentials = new AuthenticatorCredentials(oAuthInfo,
                                                                        this.getConnection().getConnectTimeout(),
                                                                        this.connection.getProxyUrl());
    try {
      PartnerConnection partnerConnection = new PartnerConnection(Authenticator.createConnectorConfig(credentials));
      SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromName(this.getSObject(), credentials);
      return SObjectsDescribeResult.of(partnerConnection,
                                       sObjectDescriptor.getName(), sObjectDescriptor.getFeaturedSObjects());
    } catch (ConnectionException e) {
      String errorMessage = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      collector.addFailure(String.format("There was issue communicating with Salesforce with error: %s", errorMessage
      ), null).withStacktrace(e.getStackTrace());
      throw collector.getOrThrowException();
    }
  }

  /**
   * Checks that input schema is correct. Which means:
   * 1. All fields in it are present in sObject
   * 2. Field types are in accordance with the actual types in sObject.
   *
   * @param schema input schema to check
   */
  private void validateInputSchema(Schema schema) {
    if (connection != null) {
      AuthenticatorCredentials authenticatorCredentials = connection.getAuthenticatorCredentials();
      try {
        SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromName(sObject, authenticatorCredentials);
        Schema sObjectActualSchema = SalesforceSchemaUtil.getSchema(authenticatorCredentials, sObjectDescriptor);
        SalesforceSchemaUtil.checkCompatibility(sObjectActualSchema, schema, false);
      } catch (ConnectionException e) {
        throw new InvalidStageException("There was issue communicating with Salesforce", e);
      }
    }
  }
}

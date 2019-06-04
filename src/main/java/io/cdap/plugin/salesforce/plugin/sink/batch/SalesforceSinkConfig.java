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

import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SObjectsDescribeResult;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.BaseSalesforceConfig;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Provides the configurations for {@link SalesforceBatchSink} plugin.
 */
public class SalesforceSinkConfig extends BaseSalesforceConfig {
  public static final String PROPERTY_ERROR_HANDLING = "errorHandling";
  public static final String PROPERTY_MAX_BYTES_PER_BATCH = "maxBytesPerBatch";
  public static final String PROPERTY_MAX_RECORDS_PER_BATCH = "maxRecordsPerBatch";
  public static final String PROPERTY_SOBJECT = "sObject";
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

  public SalesforceSinkConfig(String referenceName, String clientId,
                              String clientSecret, String username,
                              String password, String loginUrl, String sObject,
                              String maxBytesPerBatch, String maxRecordsPerBatch,
                              String errorHandling) {
    super(referenceName, clientId, clientSecret, username, password, loginUrl);
    this.sObject = sObject;
    this.maxBytesPerBatch = maxBytesPerBatch;
    this.maxRecordsPerBatch = maxRecordsPerBatch;
    this.errorHandling = errorHandling;
  }

  public String getSObject() {
    return sObject;
  }

  public Long getMaxBytesPerBatch() {
    try {
      return Long.parseLong(maxBytesPerBatch);
    } catch (NumberFormatException ex) {
      throw new InvalidConfigPropertyException("Unsupported value for maxBytesPerBatch: " + maxBytesPerBatch,
                                               SalesforceSinkConfig.PROPERTY_MAX_BYTES_PER_BATCH);
    }
  }

  public Long getMaxRecordsPerBatch() {
    try {
      return Long.parseLong(maxRecordsPerBatch);
    } catch (NumberFormatException ex) {
      throw new InvalidConfigPropertyException("Unsupported value for maxRecordsPerBatch: " + maxRecordsPerBatch,
                                               SalesforceSinkConfig.PROPERTY_MAX_RECORDS_PER_BATCH);
    }
  }

  public ErrorHandling getErrorHandling() {
    return ErrorHandling.fromValue(errorHandling)
      .orElseThrow(() -> new InvalidConfigPropertyException("Unsupported error handling value: " + errorHandling,
                                                            SalesforceSinkConfig.PROPERTY_ERROR_HANDLING));
  }

  public void validate(Schema schema) {
    super.validate();

    if (!containsMacro(PROPERTY_ERROR_HANDLING)) {
      // triggering getter will also trigger value validity check
      getErrorHandling();
    }

    if (!containsMacro(PROPERTY_MAX_BYTES_PER_BATCH)) {
      long maxBytesPerBatch = getMaxBytesPerBatch();

      if (maxBytesPerBatch <= 0 || maxBytesPerBatch > MAX_BYTES_PER_BATCH_LIMIT) {
        String errorMessage = String.format(
          "Unsupported value for maxBytesPerBatch: %d. Value should be between 1 and %d",
          maxBytesPerBatch, MAX_BYTES_PER_BATCH_LIMIT);

        throw new InvalidConfigPropertyException(errorMessage, SalesforceSinkConfig.PROPERTY_MAX_BYTES_PER_BATCH);
      }
    }


    if (!containsMacro(PROPERTY_MAX_RECORDS_PER_BATCH)) {
      long maxRecordsPerBatch = getMaxRecordsPerBatch();

      if (maxRecordsPerBatch <= 0 || maxRecordsPerBatch > MAX_RECORDS_PER_BATCH_LIMIT) {
        String errorMessage = String.format(
          "Unsupported value for maxRecordsPerBatch: %d. Value should be between 1 and %d",
          maxRecordsPerBatch, MAX_RECORDS_PER_BATCH_LIMIT);

        throw new InvalidConfigPropertyException(errorMessage, SalesforceSinkConfig.PROPERTY_MAX_RECORDS_PER_BATCH);
      }
    }

    validateSchema(schema);
  }

  private void validateSchema(Schema schema) {
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      throw new InvalidStageException("Sink schema must contain at least one field");
    }

    if (!canAttemptToEstablishConnection() || containsMacro(PROPERTY_SOBJECT)) {
      return;
    }

    Set<String> creatableSObjectFields = null;
    try {
      creatableSObjectFields = getCreatableSObjectFields();
    } catch (ConnectionException e) {
      throw new InvalidStageException("There was issue communicating with Salesforce", e);
    }

    Set<String> inputFields = schema.getFields()
      .stream()
      .map(Schema.Field::getName)
      .collect(Collectors.toSet());
    inputFields.removeAll(creatableSObjectFields);

    if (!inputFields.isEmpty()) {
      throw new InvalidStageException(String.format("Following schema fields: '%s' are not present " +
                                                         "or not creatable in target Salesforce sObject '%s'",
                                                       String.join(",", inputFields), this.getSObject()));
    }
  }

  private Set<String> getCreatableSObjectFields() throws ConnectionException {
    Set<String> creatableSObjectFields = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    AuthenticatorCredentials credentials = this.getAuthenticatorCredentials();
    PartnerConnection partnerConnection = new PartnerConnection(Authenticator.createConnectorConfig(credentials));

    SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromName(this.getSObject(), credentials);
    SObjectsDescribeResult describeResult = new SObjectsDescribeResult(partnerConnection,
                                                                       sObjectDescriptor.getAllParentObjects());
    for (Field field : describeResult.getFields()) {
      if (field.isCreateable()) {
        creatableSObjectFields.add(field.getName());
      }
    }
    return creatableSObjectFields;
  }
}

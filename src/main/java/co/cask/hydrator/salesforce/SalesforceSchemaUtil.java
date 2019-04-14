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
package co.cask.hydrator.salesforce;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.salesforce.authenticator.AuthenticatorCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Salesforce utils for parsing SOQL and generating schema
 */
public class SalesforceSchemaUtil {

  private static final Map<FieldType, Schema> SALESFORCE_TYPE_TO_CDAP_SCHEMA =
    new ImmutableMap.Builder<FieldType, Schema>()
    .put(FieldType._boolean, Schema.of(Schema.Type.BOOLEAN))
    .put(FieldType._int, Schema.of(Schema.Type.INT))
    .put(FieldType._long, Schema.of(Schema.Type.LONG))
    .put(FieldType._double, Schema.of(Schema.Type.DOUBLE))
    .put(FieldType.currency, Schema.of(Schema.Type.DOUBLE))
    .put(FieldType.percent, Schema.of(Schema.Type.DOUBLE))
    .put(FieldType.date, Schema.of(Schema.LogicalType.DATE))
    .put(FieldType.datetime, Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))
    .put(FieldType.time, Schema.of(Schema.LogicalType.TIME_MICROS))
    .put(FieldType.string, Schema.of(Schema.Type.STRING))
    .build();

  private static final Set<Schema.Type> SUPPORTED_TYPES =
    SALESFORCE_TYPE_TO_CDAP_SCHEMA.values().stream()
      .map(Schema::getType)
      .collect(Collectors.collectingAndThen(Collectors.toSet(), ImmutableSet::copyOf));

  private static final Set<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES =
    SALESFORCE_TYPE_TO_CDAP_SCHEMA.values().stream()
      .map(Schema::getLogicalType)
      .filter(Objects::nonNull)
      .collect(Collectors.collectingAndThen(Collectors.toSet(), ImmutableSet::copyOf));

  private static final Schema DEFAULT_SCHEMA = Schema.of(Schema.Type.STRING);

  /**
   * Connects to Salesforce and obtains description of sObjects needed to determine schema field types.
   * Based on this information, creates schema for the fields used in sObject descriptor.
   *
   * @param credentials connection credentials
   * @param sObjectDescriptor sObject descriptor
   * @return CDAP schema
   * @throws ConnectionException if unable to connect to Salesforce
   */
  public static Schema getSchema(AuthenticatorCredentials credentials, SObjectDescriptor sObjectDescriptor)
    throws ConnectionException {
    PartnerConnection partnerConnection = SalesforceConnectionUtil.getPartnerConnection(credentials);
    SObjectsDescribeResult describeResult = new SObjectsDescribeResult(partnerConnection,
      sObjectDescriptor.getAllParentObjects());

    return getSchemaWithFields(sObjectDescriptor, describeResult);
  }

  /**
   * Validates that fields from given CDAP schema are of supported schema.
   *
   * @param schema CDAP schema
   */
  public static void validateFieldSchemas(Schema schema) {
    for (Schema.Field field : Objects.requireNonNull(schema.getFields(), "Schema must have fields")) {
      Schema fieldSchema = field.getSchema();
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
      if (!SUPPORTED_TYPES.contains(fieldSchema.getType())) {
        throw new IllegalArgumentException(
          String.format("Field '%s' is of unsupported type '%s'", field.getName(), fieldSchema.getType()));
      }

      Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      if (logicalType != null && !SUPPORTED_LOGICAL_TYPES.contains(logicalType)) {
        throw new IllegalArgumentException(
          String.format("Field '%s' is of unsupported type '%s'", field.getName(), logicalType.getToken()));
      }
    }
  }

  /**
   * Checks two schemas compatibility based on the following rules:
   * <ul>
   *   <li>Actual schema must have fields indicated in the provided schema.</li>
   *   <li>Fields types in both schema must match.</li>
   *   <li>If actual schema field is required, provided schema field can be required or nullable.</li>
   *   <li>If actual schema field is nullable, provided schema field must be nullable.</li>
   * </ul>
   *
   * @param actualSchema schema calculated based on Salesforce metadata information
   * @param providedSchema schema provided in the configuration
   */
  public static void checkCompatibility(Schema actualSchema, Schema providedSchema) {
    for (Schema.Field providedField : Objects.requireNonNull(providedSchema.getFields())) {
      Schema.Field actualField = actualSchema.getField(providedField.getName());
      if (actualField == null) {
        throw new IllegalArgumentException(
          String.format("Field '%s' does not exist in Salesforce", providedField.getName()));
      }

      Schema providedFieldSchema = providedField.getSchema();
      Schema actualFieldSchema = actualField.getSchema();

      boolean isActualFieldNullable = actualFieldSchema.isNullable();
      boolean isProvidedFieldNullable = providedFieldSchema.isNullable();

      actualFieldSchema = isActualFieldNullable ? actualFieldSchema.getNonNullable() : actualFieldSchema;
      providedFieldSchema = isProvidedFieldNullable ? providedFieldSchema.getNonNullable() : providedFieldSchema;

      if (!actualFieldSchema.equals(providedFieldSchema)
        || !Objects.equals(actualFieldSchema.getLogicalType(), providedFieldSchema.getLogicalType())) {
        throw new IllegalArgumentException(
          String.format("Expected field '%s' to be of '%s', but it is of '%s'",
            providedField.getName(), providedFieldSchema, actualFieldSchema));
      }

      if (isActualFieldNullable && !isProvidedFieldNullable) {
        throw new IllegalArgumentException(String.format("Field '%s' should be nullable", providedField.getName()));
      }
    }
  }

  @VisibleForTesting
  static Schema getSchemaWithFields(SObjectDescriptor sObjectDescriptor, SObjectsDescribeResult describeResult) {
    List<Schema.Field> schemaFields = new ArrayList<>();

    for (SObjectDescriptor.FieldDescriptor fieldDescriptor : sObjectDescriptor.getFields()) {
      String parent = fieldDescriptor.hasParents() ? fieldDescriptor.getLastParent() : sObjectDescriptor.getName();
      Field field = describeResult.getField(parent, fieldDescriptor.getName());
      if (field == null) {
        throw new IllegalArgumentException(
          String.format("Field '%s' is absent in Salesforce describe result", fieldDescriptor.getFullName()));
      }
      Schema.Field schemaField = Schema.Field.of(fieldDescriptor.getFullName(), getCdapFieldSchema(field));
      schemaFields.add(schemaField);
    }

    return Schema.recordOf("output",  schemaFields);
  }

  private static Schema getCdapFieldSchema(Field field) {
    Schema fieldSchema = SALESFORCE_TYPE_TO_CDAP_SCHEMA.getOrDefault(field.getType(), DEFAULT_SCHEMA);
    return field.isNillable() ? Schema.nullableOf(fieldSchema) : fieldSchema;
  }

}

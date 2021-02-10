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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Salesforce utils for parsing SOQL and generating schema
 */
public class SalesforceSchemaUtil {

  public static final Set<FieldType> COMPOUND_FIELDS = ImmutableSet.of(FieldType.address, FieldType.location);

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

  private static final Pattern AVRO_NAME_START_PATTERN = Pattern.compile("^[A-Za-z_]");
  private static final Pattern AVRO_NAME_REPLACE_PATTERN = Pattern.compile("[^A-Za-z0-9_]");

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
    SObjectsDescribeResult describeResult = SObjectsDescribeResult.of(partnerConnection,
      sObjectDescriptor.getName(), sObjectDescriptor.getFeaturedSObjects());

    return getSchemaWithFields(sObjectDescriptor, describeResult);
  }

  /**
   * Validates that fields from given CDAP schema are of supported schema.
   *
   * @param schema CDAP schema
   * @param collector that collects failures
   */
  public static void validateFieldSchemas(Schema schema, FailureCollector collector) {
    for (Schema.Field field : Objects.requireNonNull(schema.getFields(), "Schema must have fields")) {
      Schema fieldSchema = field.getSchema();
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;

      if (!SUPPORTED_TYPES.contains(fieldSchema.getType())) {
        collector.addFailure(
          String.format("Field '%s' is of unsupported type '%s'.", field.getName(), fieldSchema.getDisplayName()),
          String.format("Supported types are: '%s'", SUPPORTED_TYPES.stream().map(Enum::name)
            .collect(Collectors.joining(", "))))
          .withOutputSchemaField(field.getName()).withInputSchemaField(field.getName());
      }

      Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      if (logicalType != null && !SUPPORTED_LOGICAL_TYPES.contains(logicalType)) {
        collector.addFailure(
          String.format("Field '%s' is of unsupported type '%s'.", field.getName(), fieldSchema.getDisplayName()),
          String.format("Supported types are: '%s'", SUPPORTED_LOGICAL_TYPES.stream().map(Enum::name)
            .collect(Collectors.joining(", "))))
          .withOutputSchemaField(field.getName()).withInputSchemaField(field.getName());
      }
    }
  }

  /**
   * Works like {@link SalesforceSchemaUtil#checkCompatibility(Schema, Schema, boolean)}
   * except that checking nullable is always on.
   *
   * @see SalesforceSchemaUtil#checkCompatibility(Schema, Schema, boolean)
   */
  public static void checkCompatibility(Schema actualSchema, Schema providedSchema) {
    checkCompatibility(actualSchema, providedSchema, true);
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
   * @param checkNullable if true, checks for nullability of fields in schema are triggered.
   */
  public static void checkCompatibility(Schema actualSchema, Schema providedSchema, boolean checkNullable) {
    for (Schema.Field providedField : Objects.requireNonNull(providedSchema.getFields())) {
      Schema.Field actualField = actualSchema.getField(providedField.getName(), true);
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

      if (checkNullable && isActualFieldNullable && !isProvidedFieldNullable) {
        throw new IllegalArgumentException(String.format("Field '%s' should be nullable", providedField.getName()));
      }
    }
  }

  public static Schema getSchemaWithFields(SObjectDescriptor sObjectDescriptor,
                                           SObjectsDescribeResult describeResult) {
    return getSchemaWithFields(sObjectDescriptor, describeResult, Collections.emptyList());
  }

  public static Schema getSchemaWithFields(SObjectDescriptor sObjectDescriptor,
                                           SObjectsDescribeResult describeResult,
                                           List<String> topLevelParents) {
    List<Schema.Field> schemaFields = new ArrayList<>();

    for (SObjectDescriptor.FieldDescriptor fieldDescriptor : sObjectDescriptor.getFields()) {

      SalesforceFunctionType functionType = fieldDescriptor.getFunctionType();
      Schema fieldSchema = null;
      if (!functionType.isConstant()) {
        List<String> allTopLevelParents = new ArrayList<>(topLevelParents);
        allTopLevelParents.add(sObjectDescriptor.getName());
        String parentsPath = fieldDescriptor.getParentsPath(allTopLevelParents);
        Field field = describeResult.getField(parentsPath, fieldDescriptor.getName());

        if (field == null) {
          throw new IllegalArgumentException(
            String.format("Field '%s' does not exist in Salesforce object '%s'.",
              fieldDescriptor.getFullName(), parentsPath));
        }

        fieldSchema = createFieldSchema(field);
      }

      Schema queryFieldSchema = functionType.getSchema(fieldSchema);
      Schema.Field schemaField = Schema.Field.of(normalizeAvroName(fieldDescriptor.getQueryName()), queryFieldSchema);
      schemaFields.add(schemaField);
    }

    /*
    Sub-query has one to many relationship with top level fields, results will be output as array of records

    Query:
      SELECT Id, Account.Name, (SELECT Id, Name, Contact.LastName FROM Account.Contacts) FROM Account

    Result:
     {
         "Id":"0011i000005o4KzAAI",
         "Name":"GenePoint",
         "Contacts":{
            "records":[
               {
                  "Id":"0031i000005c49GAAQ",
                  "Name":"Edna Frank",
                  "LastName":"Frank"
               },
               {
                  "Id":"0031i000005c49GAAZ",
                  "Name":"John Brown",
                  "LastName":"Brown"
               }
            ]
         }
      }
     */
    for (SObjectDescriptor childSObject : sObjectDescriptor.getChildSObjects()) {
      Schema childSchema = getSchemaWithFields(childSObject, describeResult,
        Collections.singletonList(sObjectDescriptor.getName()));

      String childName = normalizeAvroName(childSObject.getName());
      Schema.Field childField = Schema.Field.of(childName,
        Schema.arrayOf(Schema.recordOf(childName, Objects.requireNonNull(childSchema.getFields()))));
      schemaFields.add(childField);
    }

    return Schema.recordOf("output", schemaFields);
  }

  private static Schema createFieldSchema(Field field) {
    Schema fieldSchema = SALESFORCE_TYPE_TO_CDAP_SCHEMA.getOrDefault(field.getType(), DEFAULT_SCHEMA);
    return field.isNillable() ? Schema.nullableOf(fieldSchema) : fieldSchema;
  }

  /**
   * Normalize the given field name to be compatible with
   * <a href="https://avro.apache.org/docs/current/spec.html#names">Avro names</a>
   */
  private static String normalizeAvroName(String name) {
    String finalName = name;
    // Make sure the name starts with allowed character
    if (!AVRO_NAME_START_PATTERN.matcher(name).find()) {
      finalName = "A" + finalName;
    }

    // Replace any not allowed characters with "_".
    return AVRO_NAME_REPLACE_PATTERN.matcher(finalName).replaceAll("_");
  }
}

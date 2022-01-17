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

import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SalesforceSchemaUtilTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetSchemaWithFields() {
    String opportunity = "Opportunity";
    String account = "Account";
    String contacts = "Contacts";
    String owner = "Owner";

    List<SObjectDescriptor.FieldDescriptor> fieldDescriptors = Stream
      .of("Id", "Name", "Amount", "Percent", "ConversionRate", "IsWon", "CreatedDate", "CreatedDateTime", "CreatedTime")
      .map(name -> getFieldWithType(name, FieldType.anyType, false))
      .map(SObjectDescriptor.FieldDescriptor::new)
      .collect(Collectors.toList());

    fieldDescriptors.add(new SObjectDescriptor.FieldDescriptor(
      Arrays.asList(account, "NumberOfEmployees"), null, SalesforceFunctionType.NONE));
    fieldDescriptors.add(new SObjectDescriptor.FieldDescriptor(
      Collections.singletonList("Id"), "IdAlias", SalesforceFunctionType.NONE));
    fieldDescriptors.add(new SObjectDescriptor.FieldDescriptor(
      Collections.singletonList("CNT"), "Cnt", SalesforceFunctionType.LONG_REQUIRED));
    fieldDescriptors.add(new SObjectDescriptor.FieldDescriptor(
      Collections.singletonList("CreatedDate"), "Mx", SalesforceFunctionType.IDENTITY));
    fieldDescriptors.add(new SObjectDescriptor.FieldDescriptor(
      Collections.singletonList("CreatedDateTime"), "DayOnlyFunc", SalesforceFunctionType.DATE));
    fieldDescriptors.add(new SObjectDescriptor.FieldDescriptor(
      Collections.singletonList("Percent"), "GroupingFunc", SalesforceFunctionType.INT_REQUIRED));
    fieldDescriptors.add(new SObjectDescriptor.FieldDescriptor(
      Collections.singletonList("Amount"), "AvgFunc", SalesforceFunctionType.DOUBLE));
    fieldDescriptors.add(new SObjectDescriptor.FieldDescriptor(
      Collections.singletonList("CreatedDateTime"), "CalMonFunc", SalesforceFunctionType.INT));
    fieldDescriptors.add(new SObjectDescriptor.FieldDescriptor(
      Collections.singletonList("Amount"), "SumFunc", SalesforceFunctionType.NUMERIC));
    fieldDescriptors.add(new SObjectDescriptor.FieldDescriptor(
      Arrays.asList(account, "NumberOfEmployees"), "SumRelFunc", SalesforceFunctionType.NUMERIC));

    SObjectDescriptor childSObject = new SObjectDescriptor(contacts, Arrays.asList(
      new SObjectDescriptor.FieldDescriptor(
        Collections.singletonList("FirstName"), null, SalesforceFunctionType.NONE),
      new SObjectDescriptor.FieldDescriptor(
        Arrays.asList(owner, "Status"), null, SalesforceFunctionType.NONE)));

    SObjectDescriptor sObjectDescriptor =
      new SObjectDescriptor(opportunity, fieldDescriptors, Collections.singletonList(childSObject));

    Map<String, Field> opportunityFields = new LinkedHashMap<>();
    opportunityFields.put("Id", getFieldWithType("Id", FieldType._long, false));
    opportunityFields.put("Name", getFieldWithType("Name", FieldType.string, false));
    opportunityFields.put("Amount", getFieldWithType("Amount", FieldType.currency, true));
    opportunityFields.put("Percent", getFieldWithType("Percent", FieldType.percent, true));
    opportunityFields.put("ConversionRate", getFieldWithType("ConversionRate", FieldType._double, true));
    opportunityFields.put("IsWon", getFieldWithType("IsWon", FieldType._boolean, false));
    opportunityFields.put("CreatedDate", getFieldWithType("CreatedDate", FieldType.date, false));
    opportunityFields.put("CreatedDateTime", getFieldWithType("CreatedDateTime", FieldType.datetime, false));
    opportunityFields.put("CreatedTime", getFieldWithType("CreatedTime", FieldType.time, false));

    Map<String, Field> accountFields = new LinkedHashMap<>();
    accountFields.put("NumberOfEmployees", getFieldWithType("NumberOfEmployees", FieldType._long, false));

    Map<String, Field> contactsFields = new LinkedHashMap<>();
    contactsFields.put("FirstName", getFieldWithType("FirstName", FieldType.string, false));

    Map<String, Field> ownerFields = new LinkedHashMap<>();
    ownerFields.put("Status", getFieldWithType("Status", FieldType.string, false));

    Map<String, Map<String, Field>> holder = new HashMap<>();
    holder.put(opportunity, opportunityFields);
    holder.put(String.join(".", opportunity, account), accountFields);
    holder.put(String.join(".", opportunity, contacts), contactsFields);
    holder.put(String.join(".", opportunity, contacts, owner), ownerFields);
    SObjectsDescribeResult describeResult = SObjectsDescribeResult.of(holder);

    Schema actualSchema = SalesforceSchemaUtil.getSchemaWithFields(sObjectDescriptor, describeResult);

    Schema expectedSchema = Schema.recordOf("output",
                                            Schema.Field.of("Id", Schema.of(Schema.Type.LONG)),
                                            Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("Amount", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                            Schema.Field.of("Percent", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                            Schema.Field.of("ConversionRate", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                            Schema.Field.of("IsWon", Schema.of(Schema.Type.BOOLEAN)),
                                            Schema.Field.of("CreatedDate", Schema.of(Schema.LogicalType.DATE)),
                                            Schema.Field.of("CreatedDateTime", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                            Schema.Field.of("CreatedTime", Schema.of(Schema.LogicalType.TIME_MICROS)),
                                            Schema.Field.of("Account_NumberOfEmployees", Schema.of(Schema.Type.LONG)),
                                            Schema.Field.of("IdAlias", Schema.of(Schema.Type.LONG)),
                                            Schema.Field.of("Cnt", Schema.of(Schema.Type.LONG)),
                                            Schema.Field.of("Mx", Schema.of(Schema.LogicalType.DATE)),
                                            Schema.Field.of("DayOnlyFunc", Schema.of(Schema.LogicalType.DATE)),
                                            Schema.Field.of("GroupingFunc", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("AvgFunc", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                            Schema.Field.of("CalMonFunc", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("SumFunc", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                            Schema.Field.of("SumRelFunc", Schema.of(Schema.Type.LONG)),
                                            Schema.Field.of(contacts, Schema.arrayOf(Schema.recordOf(contacts,
                                                                                                     Schema.Field.of("FirstName", Schema.of(Schema.Type.STRING)),
                                                                                                     Schema.Field.of("Owner_Status", Schema.of(Schema.Type.STRING))))));

    Assert.assertEquals(expectedSchema, actualSchema);

    // Parse the schema as Avro schema to make sure all names are accepted by Avro
    new org.apache.avro.Schema.Parser().parse(actualSchema.toString());
  }

  @Test
  public void testValidateSupportedFieldSchemas() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("IntField", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("LongField", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("DoubleField", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                    Schema.Field.of("BooleanField", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                                    Schema.Field.of("DateField", Schema.of(Schema.LogicalType.DATE)),
                                    Schema.Field.of("TimestampField", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
                                    Schema.Field.of("TimeField", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("StringField", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("ArrayField", Schema.arrayOf(Schema.of(Schema.Type.STRING))));

    MockFailureCollector collector = new MockFailureCollector();
    SalesforceSchemaUtil.validateFieldSchemas(schema, collector);
  }

  @Test
  public void testValidateUnsupportedFieldSchema() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("IntField", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("BytesField", Schema.of(Schema.Type.BYTES)));

    MockFailureCollector collector = new MockFailureCollector();
    SalesforceSchemaUtil.validateFieldSchemas(schema, collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  @Test
  public void testCheckCompatibilitySuccess() {
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("Id", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("StartDate", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                          Schema.Field.of("ExtraField", Schema.of(Schema.Type.STRING)),
                                          Schema.Field.of("Comment", Schema.of(Schema.Type.STRING)));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("Id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("StartDate", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                            Schema.Field.of("Comment", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    SalesforceSchemaUtil.checkCompatibility(actualSchema, providedSchema);
  }

  @Test
  public void testCheckCompatibilityMissingField() {
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("Comment", Schema.of(Schema.Type.STRING)));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("Id", Schema.of(Schema.Type.INT)));

    thrown.expect(IllegalArgumentException.class);

    SalesforceSchemaUtil.checkCompatibility(actualSchema, providedSchema);
  }

  @Test
  public void testCheckCompatibilityIncorrectType() {
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("Id", Schema.of(Schema.Type.STRING)));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("Id", Schema.of(Schema.Type.INT)));

    thrown.expect(IllegalArgumentException.class);

    SalesforceSchemaUtil.checkCompatibility(actualSchema, providedSchema);
  }

  @Test
  public void testCheckCompatibilityIncorrectLogicalType() {
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("CreatedDateTime", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("CreatedDateTime", Schema.of(Schema.LogicalType.TIME_MICROS)));

    thrown.expect(IllegalArgumentException.class);

    SalesforceSchemaUtil.checkCompatibility(actualSchema, providedSchema);
  }

  @Test
  public void testCheckCompatibilityIncorrectNullability() {
    Schema actualSchema = Schema.recordOf("actualSchema",
                                          Schema.Field.of("Id", Schema.nullableOf(Schema.of(Schema.Type.INT))));

    Schema providedSchema = Schema.recordOf("providedSchema",
                                            Schema.Field.of("Id", Schema.of(Schema.Type.INT)));

    thrown.expect(IllegalArgumentException.class);

    SalesforceSchemaUtil.checkCompatibility(actualSchema, providedSchema);
  }

  private Field getFieldWithType(String name, FieldType type, boolean isNillable) {
    Field field = new Field();
    field.setName(name);
    field.setType(type);
    field.setNillable(isNillable);

    return field;
  }
}

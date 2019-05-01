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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
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

    List<SObjectDescriptor.FieldDescriptor> fieldDescriptors = Stream
      .of("Id", "Name", "Amount", "Percent", "ConversionRate", "IsWon", "CreatedDate", "CreatedDateTime", "CreatedTime")
      .map(name -> {
        Field field = new Field();
        field.setName(name);
        return field;
      })
      .map(SObjectDescriptor.FieldDescriptor::new)
      .collect(Collectors.toList());

    fieldDescriptors.add(new SObjectDescriptor.FieldDescriptor(Arrays.asList(account, "NumberOfEmployees")));
    SObjectDescriptor sObjectDescriptor = new SObjectDescriptor(opportunity, fieldDescriptors);

    Map<String, Field> opportunityFields = new LinkedHashMap<>();
    opportunityFields.put("Id", getFieldWithType(FieldType._long, false));
    opportunityFields.put("Name", getFieldWithType(FieldType.string, false));
    opportunityFields.put("Amount", getFieldWithType(FieldType.currency, true));
    opportunityFields.put("Percent", getFieldWithType(FieldType.percent, true));
    opportunityFields.put("ConversionRate", getFieldWithType(FieldType._double, true));
    opportunityFields.put("IsWon", getFieldWithType(FieldType._boolean, false));
    opportunityFields.put("CreatedDate", getFieldWithType(FieldType.date, false));
    opportunityFields.put("CreatedDateTime", getFieldWithType(FieldType.datetime, false));
    opportunityFields.put("CreatedTime", getFieldWithType(FieldType.time, false));

    Map<String, Field> accountFields = new LinkedHashMap<>();
    accountFields.put("NumberOfEmployees", getFieldWithType(FieldType._long, false));

    Map<String, Map<String, Field>> holder = new HashMap<>();
    holder.put(opportunity, opportunityFields);
    holder.put(account, accountFields);
    SObjectsDescribeResult describeResult = new SObjectsDescribeResult(holder);

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
      Schema.Field.of("Account.NumberOfEmployees", Schema.of(Schema.Type.LONG)));

    Assert.assertEquals(expectedSchema.toString(), actualSchema.toString());
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
      Schema.Field.of("StringField", Schema.of(Schema.Type.STRING)));

    SalesforceSchemaUtil.validateFieldSchemas(schema);
  }

  @Test
  public void testValidateUnsupportedFieldSchema() {
    Schema schema = Schema.recordOf("schema",
      Schema.Field.of("IntField", Schema.of(Schema.Type.INT)),
      Schema.Field.of("BytesField", Schema.of(Schema.Type.BYTES)));

    thrown.expect(IllegalArgumentException.class);

    SalesforceSchemaUtil.validateFieldSchemas(schema);
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

  private Field getFieldWithType(FieldType type, boolean isNillable) {
    Field field = new Field();
    field.setType(type);
    field.setNillable(isNillable);

    return field;
  }
}

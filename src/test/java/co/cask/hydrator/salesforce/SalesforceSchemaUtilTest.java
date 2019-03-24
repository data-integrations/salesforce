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
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SalesforceSchemaUtilTest {

  @Test
  public void testGetSchemaWithFields() {
    String opportunity = "Opportunity";
    String account = "Account";

    List<SObjectDescriptor.FieldDescriptor> fieldDescriptors = Stream
      .of("Id", "Name", "Amount", "Percent", "ConversionRate", "IsWon")
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
      Schema.Field.of("Account.NumberOfEmployees", Schema.of(Schema.Type.LONG)));

    Assert.assertEquals(expectedSchema, actualSchema);
  }

  private Field getFieldWithType(FieldType type, boolean isNillable) {
    Field field = new Field();
    field.setType(type);
    field.setNillable(isNillable);

    return field;
  }
}

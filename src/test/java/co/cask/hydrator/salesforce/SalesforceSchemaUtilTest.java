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
import co.cask.hydrator.salesforce.parser.SOQLParsingException;
import co.cask.hydrator.salesforce.parser.SalesforceQueryParser;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SalesforceSchemaUtilTest {

  @Test
  public void testGetSchemaWithFields() {
    List<String> fields = Arrays.asList(new String[]{"Id", "Name", "Amount", "Percent", "ConversionRate", "IsWon"});
    Map<String, Field> nameToField = new HashMap<>();
    nameToField.put("Id", getFieldWithType(FieldType._int));
    nameToField.put("Name", getFieldWithType(FieldType.string));
    nameToField.put("Amount", getFieldWithType(FieldType.currency));
    nameToField.put("Percent", getFieldWithType(FieldType.percent));
    nameToField.put("ConversionRate", getFieldWithType(FieldType._double));
    nameToField.put("IsWon", getFieldWithType(FieldType._boolean));

    Schema schema = SalesforceSchemaUtil.getSchemaWithFields(fields, nameToField);

    String exceptedSchema = "[{name: Id, schema: \"int\"}, {name: Name, schema: \"string\"}, " +
      "{name: Amount, schema: \"double\"}, {name: Percent, schema: \"double\"}, " +
      "{name: ConversionRate, schema: \"double\"}, {name: IsWon, schema: \"boolean\"}]";

    Assert.assertEquals(exceptedSchema, schema.getFields().toString());
  }

  @Test
  public void testSyntaxError() {
    try {
      SalesforceQueryParser.getFieldsFromQuery("SELECT Id");
      Assert.fail("Was excepted to fail with parsing error");
    } catch (SOQLParsingException ex) {
      Assert.assertTrue(ex.getMessage().contains("mismatched input '<EOF>' expecting FROM"));
    }

    try {
      SalesforceQueryParser.getFieldsFromQuery("SELECT Id FROM table something");
      Assert.fail("Was excepted to fail with parsing error");
    } catch (SOQLParsingException ex) {
      Assert.assertTrue(ex.getMessage().contains("extraneous input 'something'"));
    }
  }

  private Field getFieldWithType(FieldType type) {
    Field field = new Field();
    field.setType(type);

    return field;
  }
}

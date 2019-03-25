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
import co.cask.hydrator.salesforce.authenticator.Authenticator;
import co.cask.hydrator.salesforce.authenticator.AuthenticatorCredentials;
import co.cask.hydrator.salesforce.parser.SalesforceQueryParser;
import com.google.common.annotations.VisibleForTesting;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PartnerConnection;

import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Salesforce utils for parsing SOQL and generating schema
 */
public class SalesforceSchemaUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceSchemaUtil.class);

  /**
   * Returns Schema in cdap format from SOQL query. This method queries Salesforce SOAP api to
   * get fields and their data types.
   *
   * @param credentials credentials info
   * @param query SOQL query
   *
   * @return Schema for cdap
   *
   * @throws ConnectionException failure during connecting to Salesforce SOAP API
   */
  public static Schema getSchemaFromQuery(AuthenticatorCredentials credentials, String query)
    throws ConnectionException {

    String sObjectName = SalesforceQueryParser.getSObjectFromQuery(query);
    Map<String, Field> nameToField = getAllSObjectFieldsMap(credentials, sObjectName);

    List<String> queryFields = SalesforceQueryParser.getFieldsFromQuery(query);
    return getSchemaWithFields(queryFields, nameToField);
  }

  private static Map<String, Field> getAllSObjectFieldsMap(AuthenticatorCredentials credentials, String sObjectName)
    throws ConnectionException {
    ConnectorConfig connectorConfig = Authenticator.createConnectorConfig(credentials);

    PartnerConnection partnerConnection = new PartnerConnection(connectorConfig);
    DescribeSObjectResult describeSObjectResult = partnerConnection.describeSObject(sObjectName);

    Map<String, Field> nameToField = new HashMap<>();
    for (Field field : describeSObjectResult.getFields()) {
      nameToField.put(field.getName(), field);
    }
    return nameToField;
  }

  @VisibleForTesting
  static Schema getSchemaWithFields(List<String> queryFields, Map<String, Field> nameToField) {

    List<Schema.Field> schemaFields = new ArrayList<>(queryFields.size());
    for (String fieldName : queryFields) {
      Field salesforceField = nameToField.get(fieldName);
      Schema fieldSchema;

      if (isFieldComplex(fieldName)) {
        // Some complex functions like nested and aggregate are not supported by Bulk API
        // for relationship fields it would take too long to retrieve all fields for every sObject.
        // Describe calls are a bit slow.
        fieldSchema = Schema.of(Schema.Type.STRING);
      } else {
        if (salesforceField == null) {
          throw new IllegalArgumentException(String.format("Field '%s' is absent the sObject", fieldName));
        }

        FieldType fieldType = salesforceField.getType();
        fieldSchema = fieldTypeToCdapSchemaType(fieldType);

        if (salesforceField.isNillable()) {
          fieldSchema = Schema.nullableOf(fieldSchema);
        } else {
          fieldSchema = fieldSchema;
        }
      }

      Schema.Field schemaField = Schema.Field.of(fieldName, fieldSchema);
      schemaFields.add(schemaField);
    }

    return Schema.recordOf("output",  schemaFields);
  }

  /**
   * If this is a relationship query (join equivalent for soql)
   * or this is an aggregate function, or nested query as field,
   * don't convert it from string to anything as this is very error-prone.
   *
   * Examples of complex fields:
   * Relationship field - Account.Name
   * Nested field - (SELECT Id FROM Opportunity)
   * Aggregate field - AVG(Amount), COUNT()
   *
   *
   * @param fieldName name of the field
   * @return is field complex (relationship or aggregate or nested)
   */
  private static boolean isFieldComplex(String fieldName) {
    return (fieldName.contains("(") || fieldName.contains("."));
  }

  private static Schema fieldTypeToCdapSchemaType(FieldType fieldType) {
    switch(fieldType) {
      case _boolean:
        return Schema.of(Schema.Type.BOOLEAN);
      case _int:
      case _long:
        return Schema.of(Schema.Type.LONG);
      case _double:
      case currency:
      case percent:
        return Schema.of(Schema.Type.DOUBLE);
      case date:
        return Schema.of(Schema.LogicalType.DATE);
      case datetime:
        return Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS);
      case time:
        return Schema.of(Schema.LogicalType.TIME_MILLIS);
      default:
        return Schema.of(Schema.Type.STRING);
    }
  }
}

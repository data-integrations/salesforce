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
package io.cdap.salesforce;

import com.google.common.annotations.VisibleForTesting;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.salesforce.authenticator.AuthenticatorCredentials;

import java.util.ArrayList;
import java.util.List;

/**
 * Salesforce utils for parsing SOQL and generating schema
 */
public class SalesforceSchemaUtil {

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
      Schema.Field schemaField = Schema.Field.of(fieldDescriptor.getFullName(), getCdapSchemaField(field));
      schemaFields.add(schemaField);
    }

    return Schema.recordOf("output",  schemaFields);
  }

  private static Schema getCdapSchemaField(Field field) {
    Schema fieldSchema;
    switch (field.getType()) {
      case _boolean:
        fieldSchema = Schema.of(Schema.Type.BOOLEAN);
        break;
      case _int:
        fieldSchema = Schema.of(Schema.Type.INT);
        break;
      case _long:
        fieldSchema = Schema.of(Schema.Type.LONG);
        break;
      case _double:
      case currency:
      case percent:
        fieldSchema = Schema.of(Schema.Type.DOUBLE);
        break;
      case date:
        fieldSchema = Schema.of(Schema.LogicalType.DATE);
        break;
      case datetime:
        fieldSchema = Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS);
        break;
      case time:
        fieldSchema = Schema.of(Schema.LogicalType.TIME_MILLIS);
        break;
      default:
        fieldSchema = Schema.of(Schema.Type.STRING);
    }
    return field.isNillable() ? Schema.nullableOf(fieldSchema) : fieldSchema;
  }

}

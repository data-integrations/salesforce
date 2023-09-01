/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.plugin.salesforce.etl;

import com.sforce.async.ConcurrencyMode;
import com.sforce.soap.partner.Field;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SObjectsDescribeResult;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceFunctionType;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.SalesforceConnectorInfo;
import io.cdap.plugin.salesforce.plugin.connector.SalesforceConnectorConfig;
import io.cdap.plugin.salesforce.plugin.sink.batch.SalesforceSinkConfig;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SalesforceConnectorConfig.class, SalesforceConnectionUtil.class, SObjectDescriptor.class,
  SObjectsDescribeResult.class})
public class SalesforceBatchSinkSchemaValidation {
  private static final String REFERENCE_NAME = "SalesforceBatchSink-output";
  @Mock
  SObjectDescriptor mockDescriptor;
  @Mock
  SObjectsDescribeResult mockSObjectsDescribeResult;

  /**
   * Validates the schema fields with data type.
   */
  @Test
  public void testInputSchemaValidation() throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    AuthenticatorCredentials authenticatorCredentials = Mockito.mock(AuthenticatorCredentials.class);
    SalesforceSinkConfig salesforceSinkConfig = Mockito.spy(new SalesforceSinkConfig(REFERENCE_NAME,
      "CONSUMER_KEY", "CONSUMER_SECRET",
      "USERNAME", "PASSWORD",
      "LOGIN_URL", 30000, "sObject", "Insert", null,
      ConcurrencyMode.Parallel.name(), "1000000", "10000", "Fail on Error",
      BaseSalesforceETLTest.SECURITY_TOKEN,
      null, null, true));
    Schema schema = Schema.recordOf("output",
      Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("StageName", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("Tax", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("Amount", Schema.of(Schema.Type.INT))
    );
    Field field = new Field();
    List<SObjectDescriptor.FieldDescriptor> fields = new ArrayList<>();
    List<String> nameParts = new ArrayList<>();
    nameParts.add("Amount");
    SalesforceFunctionType salesforceFunctionType = SalesforceFunctionType.get("String");
    SObjectDescriptor.FieldDescriptor descriptor1 = new SObjectDescriptor.FieldDescriptor(nameParts, "Name",
      salesforceFunctionType);
    SObjectDescriptor.FieldDescriptor descriptor2 = new SObjectDescriptor.FieldDescriptor(nameParts, "StageName",
      salesforceFunctionType);
    SObjectDescriptor.FieldDescriptor descriptor3 = new SObjectDescriptor.FieldDescriptor(nameParts, "Tax",
      SalesforceFunctionType.DOUBLE);
    SObjectDescriptor.FieldDescriptor descriptor4 = new SObjectDescriptor.FieldDescriptor(nameParts, "Amount",
      SalesforceFunctionType.INT);
    fields.add(descriptor1);
    fields.add(descriptor2);
    fields.add(descriptor3);
    fields.add(descriptor4);

    SalesforceConnectorInfo connection = Mockito.mock(SalesforceConnectorInfo.class);
    Mockito.when(salesforceSinkConfig.getConnection()).thenReturn(connection);
    PowerMockito.when(connection.canAttemptToEstablishConnection()).thenReturn(true);

    Mockito.when(connection.getAuthenticatorCredentials()).thenReturn(authenticatorCredentials);
    OAuthInfo oAuthInfo = new OAuthInfo("accessToken", "www.instanceUrl.com");
    PowerMockito.whenNew(AuthenticatorCredentials.class)
      .withArguments(oAuthInfo, 3000, "www.proxyUrl.com")
      .thenReturn(authenticatorCredentials);
    PowerMockito.mockStatic(SObjectDescriptor.class);
    PowerMockito.when(SObjectDescriptor.fromName(Mockito.any(), Mockito.any()))
      .thenReturn(mockDescriptor);
    Mockito.when(mockSObjectsDescribeResult.getField(Mockito.anyString(), Mockito.anyString())).thenReturn(field);
    PowerMockito.when(mockDescriptor.getFields()).thenReturn(fields);
    PowerMockito.mockStatic(SObjectsDescribeResult.class);
    PowerMockito.when(SObjectsDescribeResult.of(Mockito.any(), Mockito.any(), Mockito.anySet()))
      .thenReturn(mockSObjectsDescribeResult);
    PowerMockito.doNothing().when(connection).validate(collector, oAuthInfo);
    PowerMockito.mockStatic(SalesforceConnectionUtil.class);
    Schema schemaUtil = SalesforceSchemaUtil.getSchema(authenticatorCredentials, mockDescriptor);
    salesforceSinkConfig.validate(schema, collector, oAuthInfo);
    Assert.assertEquals(4, schema.getFields().size());
    Assert.assertEquals(schemaUtil.getFields().get(2).getName(), schema.getFields().get(2).getName());
    Assert.assertEquals(schemaUtil.getFields().get(3).getSchema().getType(),
      schema.getFields().get(3).getSchema().getType());
  }


  /**
   * Validates the schema fields of different data type.
   */
  @Test
  public void testInputSchemaValidationWithDifferentFields() throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    AuthenticatorCredentials authenticatorCredentials = Mockito.mock(AuthenticatorCredentials.class);
    SalesforceSinkConfig salesforceSinkConfig = Mockito.spy(new SalesforceSinkConfig(REFERENCE_NAME,
      "CONSUMER_KEY", "CONSUMER_SECRET",
      "USERNAME", "PASSWORD",
      "LOGIN_URL", 30000, "sObject", "Insert", null,
      ConcurrencyMode.Parallel.name(), "1000000", "10000", "Fail on Error",
      BaseSalesforceETLTest.SECURITY_TOKEN,
      null, null, true));
    Schema schema = Schema.recordOf("output",
      Schema.Field.of("Name", Schema.of(Schema.Type.STRING)));
    Field field = new Field();
    List<SObjectDescriptor.FieldDescriptor> fields = new ArrayList<>();
    List<String> nameParts = new ArrayList<>();
    nameParts.add("Amount");
    SalesforceFunctionType salesforceFunctionType = SalesforceFunctionType.INT;
    SObjectDescriptor.FieldDescriptor descriptor = new SObjectDescriptor.FieldDescriptor(nameParts, "Name",
      salesforceFunctionType);
    fields.add(descriptor);

    SalesforceConnectorInfo connection = Mockito.mock(SalesforceConnectorInfo.class);
    Mockito.when(salesforceSinkConfig.getConnection()).thenReturn(connection);
    PowerMockito.when(connection.canAttemptToEstablishConnection()).thenReturn(true);

    Mockito.when(connection.getAuthenticatorCredentials()).thenReturn(authenticatorCredentials);
    OAuthInfo oAuthInfo = new OAuthInfo("accessToken", "www.instanceUrl.com");
    PowerMockito.whenNew(AuthenticatorCredentials.class)
      .withArguments(oAuthInfo, 3000, "www.proxyUrl.com")
      .thenReturn(authenticatorCredentials);
    PowerMockito.mockStatic(SObjectDescriptor.class);
    PowerMockito.when(SObjectDescriptor.fromName(Mockito.any(), Mockito.any()))
      .thenReturn(mockDescriptor);
    Mockito.when(mockSObjectsDescribeResult.getField(Mockito.anyString(), Mockito.anyString())).thenReturn(field);
    PowerMockito.when(mockDescriptor.getFields()).thenReturn(fields);
    PowerMockito.mockStatic(SObjectsDescribeResult.class);
    PowerMockito.when(SObjectsDescribeResult.of(Mockito.any(), Mockito.any(), Mockito.anySet()))
      .thenReturn(mockSObjectsDescribeResult);
    PowerMockito.doNothing().when(connection).validate(collector, oAuthInfo);
    PowerMockito.mockStatic(SalesforceConnectionUtil.class);
    try {
      salesforceSinkConfig.validate(schema, collector, oAuthInfo);
      collector.getOrThrowException();
      Assert.fail("Exception will not be thrown if dataType will be same.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Expected field '" + "Name" + "' to be of \"" + "string" + "\", but it is of \""
        + "int" + "\"", e.getMessage());

    }
  }
}

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
package io.cdap.plugin.salesforce.etl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.sforce.soap.metadata.CustomField;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.sobject.SObject;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceMultiSourceConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceMultiSourceConfigBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@inheritDoc}
 */
public class SalesforceBatchMultiSourceETLTest extends BaseSalesforceBatchSourceETLTest {

  @Test
  public void testWhiteList() throws Exception {
    String sObjectName1 = createCustomObject("IT_Multi_WL_1",
                                             new CustomField[]{createTextCustomField("Position__c")});
    String sObjectName2 = createCustomObject("IT_Multi_WL_2",
                                             new CustomField[]{createTextCustomField("City__c")});

    List<SObject> sObjects = ImmutableList.of(
      new SObjectBuilder()
        .setType(sObjectName1)
        .put("Name", "Fred")
        .put("Position__c", "DEV")
        .build(),
      new SObjectBuilder()
        .setType(sObjectName1)
        .put("Name", "Wilma")
        .put("Position__c", "QA")
        .build(),
      new SObjectBuilder()
        .setType(sObjectName2)
        .put("Name", "Pebbles")
        .put("City__c", "NY")
        .build());

    addSObjects(sObjects, false);

    List<StructuredRecord> records = getResultsForMultiSObjects(
      String.join(",", sObjectName1, sObjectName2), null);

    String sObjectNameField = SalesforceMultiSourceConfig.SOBJECT_NAME_FIELD_DEFAULT;

    Schema expectedSchema1 = Schema.recordOf("schema1",
                                             Schema.Field.of("Name",
                                                             Schema.of(Schema.Type.STRING)),
                                             Schema.Field.of("Position__c",
                                                             Schema.of(Schema.Type.STRING)),
                                             Schema.Field.of(sObjectNameField,
                                                             Schema.of(Schema.Type.STRING))
    );

    Schema expectedSchema2 = Schema.recordOf("schema2",
                                             Schema.Field.of("Name",
                                                             Schema.of(Schema.Type.STRING)),
                                             Schema.Field.of("City__c",
                                                             Schema.of(Schema.Type.STRING)),
                                             Schema.Field.of(sObjectNameField,
                                                             Schema.of(Schema.Type.STRING))
    );

    ImmutableList<StructuredRecord> expectedResults = ImmutableList.of(
      StructuredRecord.builder(expectedSchema1)
        .set("Name", "Fred")
        .set("Position__c", "DEV")
        .set(sObjectNameField, sObjectName1)
        .build(),
      StructuredRecord.builder(expectedSchema1)
        .set("Name", "Wilma")
        .set("Position__c", "QA")
        .set(sObjectNameField, sObjectName1)
        .build(),
      StructuredRecord.builder(expectedSchema2)
        .set("Name", "Pebbles")
        .set("City__c", "NY")
        .set(sObjectNameField, sObjectName2)
        .build());

    List<StructuredRecord> actualResults = records.stream()
      .map(record -> {
        // transform received structured record schema to exclude Salesforce system fields
        // and to make structured record of the schema we want to compare
        StructuredRecord.Builder builder;
        if (sObjectName2.equals(record.get(sObjectNameField))) {
          builder = StructuredRecord.builder(expectedSchema2)
            .set("City__c", record.get("City__c"));
        } else {
          builder = StructuredRecord.builder(expectedSchema1)
            .set("Position__c", record.get("Position__c"));
        }
        return builder.set("Name", record.get("Name"))
          .set(sObjectNameField, record.get(sObjectNameField))
          .build();

      })
      // sort by tablename and then by Name fields
      .sorted(Comparator.<StructuredRecord, String>comparing(record -> record.get(sObjectNameField), String::compareTo)
                .thenComparing(record -> record.get("Name"), String::compareTo))
      .collect(Collectors.toList());

    Assert.assertEquals(expectedResults.size(), actualResults.size());
    Assert.assertEquals(expectedResults, actualResults);
  }

  @Test
  public void testBlackList() throws Exception {
    String sObjectName1 = createCustomObject("IT_Multi_BL_1", null);
    String sObjectName2 = createCustomObject("IT_Multi_BL_2", null);

    List<SObject> sObjects = ImmutableList.of(
      new SObjectBuilder()
        .setType(sObjectName1)
        .put("Name", "Fred")
        .build(),
      new SObjectBuilder()
        .setType(sObjectName2)
        .put("Name", "Pebbles")
        .build());

    addSObjects(sObjects, false);

    Set<String> expectedSObjects = Sets.newHashSet(sObjectName1, sObjectName2);

    String blackList = Stream.of(partnerConnection.describeGlobal().getSobjects())
      .filter(DescribeGlobalSObjectResult::getQueryable)
      .map(DescribeGlobalSObjectResult::getName)
      .filter(name -> !expectedSObjects.contains(name))
      .collect(Collectors.joining(","));

    List<StructuredRecord> records = getResultsForMultiSObjects(null, blackList);

    Set<String> actualSObjects = records.stream()
      .map(record -> (String) record.get(SalesforceMultiSourceConfig.SOBJECT_NAME_FIELD_DEFAULT))
      .collect(Collectors.toSet());

    Assert.assertEquals(expectedSObjects, actualSObjects);
  }

  @Test
  public void testValidWhiteListSObjects() {
    SalesforceMultiSourceConfig salesforceMultiSourceConfig = new SalesforceMultiSourceConfigBuilder()
      .setConsumerKey(CONSUMER_KEY).setConsumerSecret(CONSUMER_SECRET).setUsername(USERNAME).setPassword(PASSWORD)
      .setSecurityToken(SECURITY_TOKEN).setLoginUrl(LOGIN_URL).setWhiteList("Account,Contact").build();

    MockFailureCollector collector = new MockFailureCollector();
    salesforceMultiSourceConfig.validateSObjects(collector);
    Assert.assertEquals(collector.getValidationFailures().size(), 0);
  }

  @Test
  public void testValidBlackListSObjects() {
    SalesforceMultiSourceConfig salesforceMultiSourceConfig = new SalesforceMultiSourceConfigBuilder()
      .setConsumerKey(CONSUMER_KEY).setConsumerSecret(CONSUMER_SECRET).setUsername(USERNAME).setPassword(PASSWORD)
      .setSecurityToken(SECURITY_TOKEN).setLoginUrl(LOGIN_URL).setBlackList("Contact").build();

    MockFailureCollector collector = new MockFailureCollector();
    salesforceMultiSourceConfig.validateSObjects(collector);
    Assert.assertEquals(collector.getValidationFailures().size(), 0);
  }

  @Test
  public void testInvalidWhiteListSObjects() {
    SalesforceMultiSourceConfig salesforceMultiSourceConfig = new SalesforceMultiSourceConfigBuilder()
      .setConsumerKey(CONSUMER_KEY).setConsumerSecret(CONSUMER_SECRET).setUsername(USERNAME).setPassword(PASSWORD)
      .setSecurityToken(SECURITY_TOKEN).setLoginUrl(LOGIN_URL).setWhiteList("invalidObject1,invalidObject2").build();

    MockFailureCollector collector = new MockFailureCollector();
    salesforceMultiSourceConfig.validateSObjects(collector);
    Assert.assertEquals(collector.getValidationFailures().size(), 1);
  }

  @Test
  public void testInvalidBlackListSObjects() {

    SalesforceMultiSourceConfig salesforceMultiSourceConfig = new SalesforceMultiSourceConfigBuilder()
      .setConsumerKey(CONSUMER_KEY).setConsumerSecret(CONSUMER_SECRET).setUsername(USERNAME).setPassword(PASSWORD)
      .setSecurityToken(SECURITY_TOKEN).setLoginUrl(LOGIN_URL).setBlackList("invalidObject1,invalidObject2").build();

    MockFailureCollector collector = new MockFailureCollector();
    salesforceMultiSourceConfig.validateSObjects(collector);
    Assert.assertEquals(collector.getValidationFailures().size(), 1);
  }
}

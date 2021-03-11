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
package io.cdap.plugin.salesforce.parser;

import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceFunctionType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class SalesforceQueryParserTest {
  private static final Map<String, Object[]> queries = new HashMap<>();

  static {
    queries.put("SELECT Id, Name FROM Opportunity",
                new Object[]{"Opportunity", new String[]{"Id", "Name"}});

    queries.put("SELECT Id FROM Contact WHERE Name LIKE 'A%' AND MailingCity = 'California'",
                new Object[]{"Contact", new String[]{"Id"}});

    queries.put("SELECT Name FROM Account ORDER BY Name DESC NULLS LAST",
                new Object[]{"Account", new String[]{"Name"}});

    queries.put("SELECT Name FROM Account WHERE Industry = 'media' LIMIT 125",
                new Object[]{"Account", new String[]{"Name"}});

    queries.put("SELECT Name FROM Account WHERE Industry = 'media' ORDER BY BillingPostalCode ASC NULLS LAST LIMIT 125",
                new Object[]{"Account", new String[]{"Name"}});

    queries.put("SELECT Name, Id FROM Merchandise__c ORDER BY Name OFFSET 100",
                new Object[]{"Merchandise__c", new String[]{"Name", "Id"}});

    queries.put("SELECT Name, Id FROM Merchandise__c ORDER BY Name LIMIT 20 OFFSET 100",
                new Object[]{"Merchandise__c", new String[]{"Name", "Id"}});

    queries.put("SELECT Id, Name, Account.Name FROM Contact WHERE Account.Industry = 'media'",
                new Object[]{"Contact", new String[]{"Id", "Name", "Account.Name"}});

    queries.put("SELECT Id, FirstName__c, FirstName__c FROM Daughter__c WHERE Mother_of_Child__r.LastName__c LIKE 'C%'",
                new Object[]{"Daughter__c", new String[]{"Id", "FirstName__c", "FirstName__c"}});

    queries.put("SELECT Id, Who.FirstName, Who.LastName FROM Task WHERE Owner.FirstName LIKE 'B%'",
                new Object[]{"Task", new String[]{"Id", "Who.FirstName", "Who.LastName"}});

    queries.put("SELECT UserId, LoginTime from LoginHistory",
                new Object[]{"LoginHistory", new String[]{"UserId", "LoginTime"}});

    queries.put("SELECT LoginHistory.UserId, LoginHistory.LoginTime from LoginHistory",
      new Object[]{"LoginHistory", new String[]{"UserId", "LoginTime"}});

    queries.put("SELECT lh.UserId, lh.LoginTime from LoginHistory lh",
      new Object[]{"LoginHistory", new String[]{"UserId", "LoginTime"}});

    queries.put("SELECT lh.UserId, lh.LoginTime from LoginHistory AS lh",
      new Object[]{"LoginHistory", new String[]{"UserId", "LoginTime"}});

    queries.put("SELECT UserId from LoginHistory WHERE LoginTime > " +
                  "2010-09-20T22:16:30.000Z AND LoginTime < 2010-09-21T22:16:30.000Z",
                new Object[]{"LoginHistory", new String[]{"UserId"}});

    queries.put("SELECT HoursUntilExpiration, Category, IsPublished FROM ActionLinkGroupTemplate",
      new Object[]{"ActionLinkGroupTemplate", new String[]{"HoursUntilExpiration", "Category", "IsPublished"}});

    queries.put("SELECT Id, Name, Domain FROM Domain",
      new Object[]{"Domain", new String[]{"Id", "Name", "Domain"}});
  }

  @Test
  public void testGetSObjectFromQuery() {
    for (Map.Entry<String, Object[]> entry : SalesforceQueryParserTest.queries.entrySet()) {
      String query = entry.getKey();
      String expectedSObjectName = (String) entry.getValue()[0];

      SObjectDescriptor actualSObjectName = SalesforceQueryParser.getObjectDescriptorFromQuery(query);
      Assert.assertEquals(expectedSObjectName, actualSObjectName.getName());
    }
  }

  @Test
  public void testGetFieldsFromQuery() {
    for (Map.Entry<String, Object[]> entry : SalesforceQueryParserTest.queries.entrySet()) {
      String query = entry.getKey();
      List<String> expectedFields = Arrays.asList((String[]) entry.getValue()[1]);

      SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(query);
      List<String> actualFields =  sObjectDescriptor.getFieldsNames();
      Assert.assertEquals(expectedFields, actualFields);
    }
  }

  @Test
  public void testQueryParseError() {
    Stream.of(
      "SELECT COUNT() FROM Contact",
      "SELECT LeadSource, COUNT(Name) FROM Lead GROUP BY LeadSource",
      "SELECT Name, COUNT(Id) FROM Account GROUP BY Name HAVING COUNT(Id) > 1",
      "SELECT NAME n from Account",
      "SELECT * from Account",
      "SELECT TYPEOF What WHEN Account THEN Phone ELSE Email END FROM Event",
      "SELECT Name FROM Contact.Account",
      "SELECT COUNT() from Opportunity",
      "SELECT COUNT_DISTINCT(Company) FROM Lead",
      "SELECT HOUR_IN_DAY(convertTimezone(CreatedDate)) FROM Opportunity",
      "SELECT Account.Name FROM Opportunity GROUP BY Account.Name")
      .forEach(query -> {
        try {
          SObjectDescriptor.fromQuery(query);
          Assert.fail(String.format("Error must be thrown during query '%s' parsing", query));
        } catch (SOQLParsingException e) {
          // expected failure, do nothing
        }
      });
  }

  @Test
  public void testFromStatement() {
    String fromStatement = "FROM Contact WHERE Account.Industry = 'media'";
    String query = "SELECT Id, Name, Account.Name " + fromStatement;

    String result = SalesforceQueryParser.getFromStatement(query);
    Assert.assertEquals(fromStatement, result);
  }

  @Test
  public void testRestrictedQuery() {
    bulkApiRestrictedQuery();
  }

  @Test
  public void testPKRestrictedQuery() {
    bulkApiRestrictedQuery();
    Stream.of(
      "SELECT Name FROM Opportunity LIMIT 10",
      "SELECT Id FROM UserProfileFeed WITH UserId='005D0000001AamR'",
      "SELECT Title FROM KnowledgeArticleVersion WHERE PublishStatus='online' WITH DATA CATEGORY Geography__c " +
        "ABOVE usa__c",
      "SELECT Name FROM Account ORDER BY Name DESC",
      "SELECT Name, ID FROM Contact FOR REFERENCE",
      "SELECT Name, ID FROM Contact FOR VIEW",
      "SELECT Id FROM Account FOR UPDATE"
    )
      .forEach(query -> Assert.assertTrue(String.format("Query '%s' should have been restricted", query),
                                          SalesforceQueryParser.isRestrictedPKQuery(query)));
  }

  private void bulkApiRestrictedQuery() {
    Stream.of(
      "SELECT MAX(CloseDate) Amt FROM Opportunity",
      "SELECT Name n, MAX(Amount) max FROM Opportunity GROUP BY Name",
      "SELECT MAX(Amount) max, Name n FROM Opportunity GROUP BY Name",
      "SELECT Name, (SELECT LastName FROM Contacts) FROM Account",
      "SELECT LeadSource FROM Lead GROUP BY LeadSource",
      "SELECT LeadSource, COUNT(Name) cnt FROM Lead GROUP BY ROLLUP(LeadSource)",
      "SELECT Type, BillingCountry, GROUPING(Type) grpType, GROUPING(BillingCountry) grpCty, COUNT(id) accts " +
        "FROM Account GROUP BY CUBE(Type, BillingCountry) ORDER BY GROUPING(Type), GROUPING(BillingCountry)",
      "SELECT Name FROM Opportunity LIMIT 10 OFFSET 2")
      .forEach(query -> Assert.assertTrue(String.format("Query '%s' should have been restricted", query),
                                          SalesforceQueryParser.isRestrictedQuery(query)));
  }

  @Test
  public void testNotRestrictedQuery() {
    Stream.of(
      "SELECT Id, Name FROM Opportunity",
      "SELECT Name, Id FROM Merchandise__c ORDER BY Name",
      "SELECT Id, Who.FirstName, Who.LastName FROM Task WHERE Owner.FirstName LIKE 'B%'",
      "SELECT Name FROM Account ORDER BY Name DESC NULLS LAST")
      .forEach(query -> Assert.assertFalse(String.format("Query '%s' should have not been restricted", query),
        SalesforceQueryParser.isRestrictedQuery(query)));
  }

  @Test
  public void testConstantFunctionCall() {
    String query = "SELECT COUNT() CNT from Opportunity";

    SObjectDescriptor expectedResult = new SObjectDescriptor("Opportunity",
      Collections.singletonList(new SObjectDescriptor.FieldDescriptor(
        Collections.singletonList("CNT"), "CNT", SalesforceFunctionType.LONG_REQUIRED)));

    Assert.assertEquals(expectedResult, SalesforceQueryParser.getObjectDescriptorFromQuery(query));
  }

  @Test
  public void testIdentityFunctionCall() {
    String query = "SELECT MAX(CloseDate) MX FROM Opportunity";

    SObjectDescriptor expectedResult = new SObjectDescriptor("Opportunity",
      Collections.singletonList(new SObjectDescriptor.FieldDescriptor(
        Collections.singletonList("CloseDate"), "MX", SalesforceFunctionType.IDENTITY)));

    Assert.assertEquals(expectedResult, SalesforceQueryParser.getObjectDescriptorFromQuery(query));
  }

  @Test
  public void testNestedFunctionCall() {
    String query = "SELECT HOUR_IN_DAY(convertTimezone(CreatedDate)) HID "
      + "FROM Opportunity GROUP BY HOUR_IN_DAY(convertTimezone(CreatedDate))";

    SObjectDescriptor expectedResult = new SObjectDescriptor("Opportunity",
      Collections.singletonList(new SObjectDescriptor.FieldDescriptor(
        Collections.singletonList("CreatedDate"), "HID", SalesforceFunctionType.INT)));

    Assert.assertEquals(expectedResult, SalesforceQueryParser.getObjectDescriptorFromQuery(query));
  }

  @Test
  public void testFunctionCallWithRelationshipField() {
    String query = "SELECT MAX(Account.Name) MX FROM Contact";

    SObjectDescriptor expectedResult = new SObjectDescriptor("Contact",
      Collections.singletonList(new SObjectDescriptor.FieldDescriptor(
        Arrays.asList("Account", "Name"), "MX", SalesforceFunctionType.IDENTITY)));

    Assert.assertEquals(expectedResult, SalesforceQueryParser.getObjectDescriptorFromQuery(query));
  }

  @Test
  public void testFunctionCallWithAlias() {
    String query = "SELECT MAX(Contact.Account.Name) MX FROM Contact";

    SObjectDescriptor expectedResult = new SObjectDescriptor("Contact",
      Collections.singletonList(new SObjectDescriptor.FieldDescriptor(
        Arrays.asList("Account", "Name"), "MX", SalesforceFunctionType.IDENTITY)));

    Assert.assertEquals(expectedResult, SalesforceQueryParser.getObjectDescriptorFromQuery(query));
  }

  @Test
  public void testSubQuery() {
    String query = "SELECT Name, (SELECT FirstName, LastName, Owner.Id FROM Contacts) FROM Account";
    SObjectDescriptor result = SalesforceQueryParser.getObjectDescriptorFromQuery(query);

    Assert.assertEquals(Collections.singletonList(new SObjectDescriptor.FieldDescriptor(
      Collections.singletonList("Name"), null, SalesforceFunctionType.NONE)),
      result.getFields());

    List<SObjectDescriptor.FieldDescriptor> fields = Arrays.asList(
      new SObjectDescriptor.FieldDescriptor(
        Collections.singletonList("FirstName"), null, SalesforceFunctionType.NONE),
      new SObjectDescriptor.FieldDescriptor(
        Collections.singletonList("LastName"), null, SalesforceFunctionType.NONE),
      new SObjectDescriptor.FieldDescriptor(
        Arrays.asList("Owner", "Id"), null, SalesforceFunctionType.NONE)
    );

    Assert.assertEquals(
      Collections.singletonList(new SObjectDescriptor("Contacts", fields)),
      result.getChildSObjects());
  }
}

/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.salesforcebatchsource.actions;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBatchSource;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceSourceConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceSourceConfigBuilder;
import io.cdap.plugin.salesforcebatchsource.locators.SalesforcePropertiesPage;
import io.cdap.plugin.utils.MiscUtils;
import io.cdap.plugin.utils.SchemaFieldTypeMapping;
import io.cdap.plugin.utils.SchemaTable;
import io.cdap.plugin.utils.enums.SOQLQueryType;
import io.cdap.plugin.utils.enums.SObjects;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Salesforce source plugins - Actions.
 */
public class SalesforcePropertiesPageActions {
  private static final Logger logger = LoggerFactory.getLogger(SalesforcePropertiesPageActions.class);

  static {
    SeleniumHelper.getPropertiesLocators(SalesforcePropertiesPage.class);
  }

  public static void fillReferenceName(String referenceName) {
    logger.info("Fill Reference name: " + referenceName);
    SalesforcePropertiesPage.referenceNameInput.sendKeys(referenceName);
  }

  public static void fillAuthenticationProperties(String username, String password, String securityToken,
                                                  String consumerKey, String consumerSecret) {
    logger.info("Fill Authentication properties for user: " + username);
    SalesforcePropertiesPage.usernameInput.sendKeys(username);
    SalesforcePropertiesPage.passwordInput.sendKeys(password);
    SalesforcePropertiesPage.securityTokenInput.sendKeys(securityToken);
    SalesforcePropertiesPage.consumerKeyInput.sendKeys(consumerKey);
    SalesforcePropertiesPage.consumerSecretInput.sendKeys(consumerSecret);
  }

  public static void fillAuthenticationPropertiesForSalesforceAdminUser() {
    SalesforcePropertiesPageActions.fillAuthenticationProperties(
      System.getenv("SALESFORCE_USERNAME"),
      System.getenv("SALESFORCE_PASSWORD"),
      System.getenv("SALESFORCE_SECURITY_TOKEN"),
      System.getenv("SALESFORCE_CONSUMER_KEY"),
      System.getenv("SALESFORCE_CONSUMER_SECRET"));
  }

  public static void fillAuthenticationPropertiesWithInvalidValues() {
    SalesforcePropertiesPageActions.fillAuthenticationProperties(
      PluginPropertyUtils.pluginProp("invalid.admin.username"),
      PluginPropertyUtils.pluginProp("invalid.admin.password"),
      PluginPropertyUtils.pluginProp("invalid.admin.security.token"),
      PluginPropertyUtils.pluginProp("invalid.admin.consumer.key"),
      PluginPropertyUtils.pluginProp("invalid.admin.consumer.secret"));
  }

  public static void fillSOQLPropertyField(SOQLQueryType queryType) {
    logger.info("Fill SOQL Query field for Type: " + queryType + " using the Query: " + queryType.query);
    SalesforcePropertiesPage.soqlTextarea.sendKeys(queryType.query);
  }

  public static void configureSalesforcePluginForSoqlQuery(SOQLQueryType queryType) {
    String referenceName = "TestSF" + RandomStringUtils.randomAlphanumeric(7);
    fillReferenceName(referenceName);
    fillSOQLPropertyField(queryType);
  }

  public static void fillSObjectName(String sObjectName) {
    SalesforcePropertiesPage.sObjectNameInput.sendKeys(sObjectName);
  }

  public static void configureSalesforcePluginForSObjectQuery(SObjects sObjectName) {
    String referenceName = "TestSF" + RandomStringUtils.randomAlphanumeric(7);
    fillReferenceName(referenceName);
    fillSObjectName(sObjectName.value);
  }
  private static AuthenticatorCredentials setAuthenticationCredentialsOfAdminUser() {
    return new AuthenticatorCredentials(PluginPropertyUtils.pluginProp("admin.username"),
      PluginPropertyUtils.pluginProp("admin.password"),
      PluginPropertyUtils.pluginProp("admin.consumer.key"),
      PluginPropertyUtils.pluginProp("admin.consumer.secret"),
      PluginPropertyUtils.pluginProp("login.url"), null, null
    );
  }

  private static SalesforceSourceConfig getSourceConfigWithSOQLQuery(SOQLQueryType queryType) {
    String referenceName = "TestSF" + RandomStringUtils.randomAlphanumeric(7);
    AuthenticatorCredentials adminUserAuthCreds = setAuthenticationCredentialsOfAdminUser();

    return new SalesforceSourceConfigBuilder()
      .setReferenceName(referenceName)
      .setUsername(adminUserAuthCreds.getUsername())
      .setPassword(adminUserAuthCreds.getPassword())
      .setConsumerKey(adminUserAuthCreds.getConsumerKey())
      .setConsumerSecret(adminUserAuthCreds.getConsumerSecret())
      .setSecurityToken(PluginPropertyUtils.pluginProp("security.token"))
      .setLoginUrl(adminUserAuthCreds.getLoginUrl())
      .setQuery(queryType.query)
      .build();
  }

  public static SchemaTable getExpectedSchemaTableForSOQLQuery(SOQLQueryType queryType) {
    SalesforceBatchSource batchSource = new SalesforceBatchSource(getSourceConfigWithSOQLQuery(queryType));
    OAuthInfo oAuthInfo = null;
    try {
      oAuthInfo =
              Authenticator.getOAuthInfo(getSourceConfigWithSOQLQuery(queryType).getConnection()
                      .getAuthenticatorCredentials());
    } catch (Exception e) {
      String message = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      throw  new RuntimeException("Error encountered while establishing connection: " + message);
    }
    Schema expectedSchema = batchSource.retrieveSchema(oAuthInfo);
    return getExpectedSchemaTableFromSchema(expectedSchema);
  }

  private static SalesforceSourceConfig getSourceConfigWithSObjectName(SObjects sObjectName) {
    String referenceName = "TestSF" + RandomStringUtils.randomAlphanumeric(7);
    AuthenticatorCredentials adminUserAuthCreds = setAuthenticationCredentialsOfAdminUser();

    return new SalesforceSourceConfigBuilder()
      .setReferenceName(referenceName)
      .setUsername(adminUserAuthCreds.getUsername())
      .setPassword(adminUserAuthCreds.getPassword())
      .setConsumerKey(adminUserAuthCreds.getConsumerKey())
      .setConsumerSecret(adminUserAuthCreds.getConsumerSecret())
      .setSecurityToken(PluginPropertyUtils.pluginProp("security.token"))
      .setLoginUrl(adminUserAuthCreds.getLoginUrl())
      .setSObjectName(sObjectName.value)
      .build();
  }

  public static SchemaTable getExpectedSchemaTableForSObjectQuery(SObjects sObjectName) {
    SalesforceBatchSource batchSource = new SalesforceBatchSource(getSourceConfigWithSObjectName(sObjectName));
    OAuthInfo oAuthInfo = null;
    try {
      oAuthInfo =
              Authenticator.getOAuthInfo(getSourceConfigWithSObjectName(sObjectName).getConnection()
                      .getAuthenticatorCredentials());
    } catch (Exception e) {
      String message = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      throw  new RuntimeException("Error encountered while establishing connection: " + message);
    }
    Schema expectedSchema = batchSource.retrieveSchema(oAuthInfo);
    return getExpectedSchemaTableFromSchema(expectedSchema);
  }

  public static SchemaTable getExpectedSchemaTableFromSchema(Schema expectedSchema) {
    SchemaTable expectedSchemaTable = new SchemaTable();
    List<Schema.Field> schemaFields = expectedSchema.getFields();

    for (Schema.Field field : schemaFields) {
      String fieldName = field.getName();
      String fieldTypeInSchema = field.getSchema().toString();
      String fieldType;

      if (fieldTypeInSchema.contains("logicalType") && fieldTypeInSchema.contains("date")) {
        fieldType = "date";
      } else if (fieldTypeInSchema.contains("logicalType") && fieldTypeInSchema.contains("timestamp")) {
        fieldType = "timestamp";
      } else {
        fieldType = MiscUtils.getSubStringBetweenDoubleQuotes(fieldTypeInSchema);
      }

      expectedSchemaTable.addField(new SchemaFieldTypeMapping(fieldName, fieldType));
    }

    return expectedSchemaTable;
  }

  public static void verifyIfRecordCreatedInSinkForObjectIsCorrect(String expectedOutputFile) {
  }

  public static void clickOnServicenowConnection() {
    String connectionName = PluginPropertyUtils.pluginProp("connection.name");
    ElementHelper.clickOnElement(CdfPluginPropertiesLocators.locateElementContainingText(connectionName));
  }
}

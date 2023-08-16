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
import io.cdap.e2e.utils.AssertionHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
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
import io.cdap.plugin.utils.enums.SalesforceBatchSourceProperty;
import org.apache.commons.lang3.RandomStringUtils;
import org.openqa.selenium.WebElement;
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
    SalesforcePropertiesPage.referenceInput.sendKeys(referenceName);
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

  public static void clickOnGetSchemaButton() {
    logger.info("Click on the Get Schema button");
    SalesforcePropertiesPage.getSchemaButton.click();
    WaitHelper.waitForElementToBeDisplayed(SalesforcePropertiesPage.loadingSpinnerOnGetSchemaButton);
    WaitHelper.waitForElementToBeHidden(SalesforcePropertiesPage.loadingSpinnerOnGetSchemaButton);
    WaitHelper.waitForElementToBeDisplayed(SalesforcePropertiesPage.getSchemaButton);
  }

  public static void clickOnValidateButton() {
    logger.info("Click on the Validate button");
    SalesforcePropertiesPage.validateButton.click();
    WaitHelper.waitForElementToBeDisplayed(SalesforcePropertiesPage.loadingSpinnerOnValidateButton);
    WaitHelper.waitForElementToBeHidden(SalesforcePropertiesPage.loadingSpinnerOnValidateButton);
    WaitHelper.waitForElementToBeDisplayed(SalesforcePropertiesPage.validateButton);
  }

  public static void verifyNoErrorsFoundSuccessMessage() {
    AssertionHelper.verifyElementDisplayed(SalesforcePropertiesPage.noErrorsFoundSuccessMessage);
  }

  private static AuthenticatorCredentials setAuthenticationCredentialsOfAdminUser() {
    return new AuthenticatorCredentials(PluginPropertyUtils.pluginProp("admin.username"),
      PluginPropertyUtils.pluginProp("admin.password"),
      PluginPropertyUtils.pluginProp("admin.consumer.key"),
      PluginPropertyUtils.pluginProp("admin.consumer.secret"),
      PluginPropertyUtils.pluginProp("login.url"), null, null, null
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

  public static void verifyFieldTypeMappingDisplayed(SchemaFieldTypeMapping schemaFieldTypeMapping) {
    WebElement element = SalesforcePropertiesPage.getSchemaFieldTypeMappingElement(schemaFieldTypeMapping);
    AssertionHelper.verifyElementDisplayed(element);
  }

  public static void verifyOutputSchemaTable(SchemaTable schemaTable) {
    List<SchemaFieldTypeMapping> listOfFields = schemaTable.getListOfFields();

    for (SchemaFieldTypeMapping schemaFieldTypeMapping : listOfFields) {
      verifyFieldTypeMappingDisplayed(schemaFieldTypeMapping);
    }
  }

  public static void clickOnClosePropertiesPageButton() {
    logger.info("Close the Salesforce (batch) source properties page");
    SalesforcePropertiesPage.closePropertiesPageButton.click();
  }

  public static void verifyRequiredFieldsMissingValidationMessage(SalesforceBatchSourceProperty propertyName) {
    WebElement element = SalesforcePropertiesPage.getPropertyInlineErrorMessage(propertyName);

    AssertionHelper.verifyElementDisplayed(element);
    AssertionHelper.verifyElementContainsText(element, propertyName.propertyMissingValidationMessage);
  }

  public static void verifyPropertyInlineErrorMessage(SalesforceBatchSourceProperty property,
                                                      String expectedErrorMessage) {
    WebElement element = SalesforcePropertiesPage.getPropertyInlineErrorMessage(property);

    AssertionHelper.verifyElementDisplayed(element);
    AssertionHelper.verifyElementContainsText(element, expectedErrorMessage);
  }

  public static void verifyInvalidSoqlQueryErrorMessageForStarQueries() {
    verifyPropertyInlineErrorMessage(SalesforceBatchSourceProperty.SOQL_QUERY,
      PluginPropertyUtils.errorProp("invalid.soql.starquery"));
  }

  public static void verifyErrorMessageOnHeader(String expectedErrorMessage) {
    AssertionHelper.verifyElementContainsText(SalesforcePropertiesPage.errorMessageOnHeader,
      expectedErrorMessage);
  }

  public static void verifyValidationMessageForBlankAuthenticationProperty() {
    verifyErrorMessageOnHeader(PluginPropertyUtils.errorProp("empty.authentication.property"));
  }

  public static void verifyValidationMessageForInvalidAuthenticationProperty() {
    verifyErrorMessageOnHeader(PluginPropertyUtils.errorProp("invalid.authentication.property"));
  }

  public static void verifyValidationMessageForMissingSoqlOrSobjectNameProperty() {
    verifyErrorMessageOnHeader(PluginPropertyUtils.errorProp("required.property.soqlorsobjectname.error"));
    verifyRequiredFieldsMissingValidationMessage(SalesforceBatchSourceProperty.SOQL_QUERY);
  }

  public static void verifyValidationMessageForInvalidSObjectName(String invalidSObjectName) {
    String expectedValidationMessage = PluginPropertyUtils.errorProp(
      "invalid.sobjectname.error") + " '" + invalidSObjectName + "''";
    verifyErrorMessageOnHeader(expectedValidationMessage);
  }
}

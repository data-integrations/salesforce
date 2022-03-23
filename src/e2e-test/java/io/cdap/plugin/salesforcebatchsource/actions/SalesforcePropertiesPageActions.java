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
import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfPluginPropertiesActions;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.AssertionHelper;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
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
import org.openqa.selenium.WebElement;
import java.io.IOException;
import java.util.List;

/**
 * Salesforce source plugins - Actions.
 */
public class SalesforcePropertiesPageActions {

  static {
    SeleniumHelper.getPropertiesLocators(SalesforcePropertiesPage.class);
  }

  public static void fillReferenceName(String referenceName) {
    ElementHelper.sendKeys(SalesforcePropertiesPage.referenceNameInput, referenceName);
  }

  public static void fillAuthenticationProperties(String username, String password, String securityToken,
                                                  String consumerKey, String consumerSecret) {

    ElementHelper.sendKeys(SalesforcePropertiesPage.usernameInput, username);
    ElementHelper.sendKeys(SalesforcePropertiesPage.passwordInput, password);
    ElementHelper.sendKeys(SalesforcePropertiesPage.securityTokenInput, securityToken);
    ElementHelper.sendKeys(SalesforcePropertiesPage.consumerKeyInput, consumerKey);
    ElementHelper.sendKeys(SalesforcePropertiesPage.consumerSecretInput, consumerSecret);
  }

  public static void fillAuthenticationPropertiesForSalesforceAdminUser() {
    SalesforcePropertiesPageActions.fillAuthenticationProperties(
      PluginPropertyUtils.pluginProp("admin.username"),
      PluginPropertyUtils.pluginProp("admin.password"),
      PluginPropertyUtils.pluginProp("admin.security.token"),
      PluginPropertyUtils.pluginProp("admin.consumer.key"),
      PluginPropertyUtils.pluginProp("admin.consumer.secret"));
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
      SalesforcePropertiesPage.soqlTextarea.sendKeys(queryType.query);
  }

  public static void configureSalesforcePluginForSoqlQuery(SOQLQueryType queryType) {
    String referenceName = "TestSF" + RandomStringUtils.randomAlphanumeric(7);
    fillReferenceName(referenceName);
    fillSOQLPropertyField(queryType);
  }

  public static void fillSObjectName(String sObjectName) {
    ElementHelper.sendKeys(SalesforcePropertiesPage.sObjectNameInput, sObjectName);
  }

  public static void configureSalesforcePluginForSObjectQuery(SObjects sObjectName) {
    String referenceName = "TestSF" + RandomStringUtils.randomAlphanumeric(7);
    fillReferenceName(referenceName);
    fillSObjectName(sObjectName.value);
  }

  private static AuthenticatorCredentials setAuthenticationCredentialsOfAdminUser() {
    return new AuthenticatorCredentials(
      PluginPropertyUtils.pluginProp("admin.username"),
      PluginPropertyUtils.pluginProp("admin.password"),
      PluginPropertyUtils.pluginProp("admin.consumer.key"),
      PluginPropertyUtils.pluginProp("admin.consumer.secret"),
      PluginPropertyUtils.pluginProp("login.url")
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
    Schema expectedSchema = batchSource.retrieveSchema();
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
    Schema expectedSchema = batchSource.retrieveSchema();
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
    SalesforcePropertiesPage.closePropertiesPageButton.click();
  }
  public static void fillLastModifyAfter(String lastModifyafterLocation) {
    ElementHelper.clearElementValue(SalesforcePropertiesPage.lastModifiedAfterInput);
    ElementHelper.sendKeys(SalesforcePropertiesPage.lastModifiedAfterInput,
            PluginPropertyUtils.pluginProp(lastModifyafterLocation));
  }

  public static void closePluginPropertiesPage() {
    CdfPluginPropertiesActions.clickCloseButton();
  }


  public static void clickPreviewAndConfigure() {
    CdfStudioActions.openPreviewMenu();
    SalesforcePropertiesPage.previewConfigRunButton.click();
  }
}

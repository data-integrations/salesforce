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

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.salesforcebatchsource.locators.SalesforcePropertiesPage;
import io.cdap.plugin.utils.enums.SOQLQueryType;
import io.cdap.plugin.utils.enums.SObjects;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public static void clickOnServicenowConnection() {
    String connectionName = PluginPropertyUtils.pluginProp("connection.name");
    ElementHelper.clickOnElement(CdfPluginPropertiesLocators.locateElementContainingText(connectionName));
  }
}

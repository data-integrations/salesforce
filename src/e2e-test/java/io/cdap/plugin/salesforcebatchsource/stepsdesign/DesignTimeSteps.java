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

package io.cdap.plugin.salesforcebatchsource.stepsdesign;

import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfPluginPropertiesActions;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.BQValidation;
import io.cdap.plugin.salesforcebatchsource.actions.SalesforcePropertiesPageActions;
import io.cdap.plugin.utils.enums.SOQLQueryType;
import io.cdap.plugin.utils.enums.SObjects;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

/**
 * Design-time steps of Salesforce plugins.
 */
public class DesignTimeSteps {

  @When("fill Reference Name property")
  public void fillReferenceNameProperty() {
    String referenceName = "TestSF" + RandomStringUtils.randomAlphanumeric(7);
    SalesforcePropertiesPageActions.fillReferenceName(referenceName);
  }

  @When("fill Authentication properties with invalid values")
  public void fillAuthenticationPropertiesWithInvalidValues() {
    SalesforcePropertiesPageActions.fillAuthenticationPropertiesWithInvalidValues();
  }

  @When("fill Authentication properties for Salesforce Admin user")
  public void fillAuthenticationPropertiesForSalesforceAdminUser() {
    SalesforcePropertiesPageActions.fillAuthenticationPropertiesForSalesforceAdminUser();
  }

  @When("Click on the Macro button of SOQL Property: {string} and set the value to: {string}")
  public void fillValueInMacroEnabledSoqlProperty(String property, String value) {
    CdfPluginPropertiesActions.clickMacroButtonOfProperty(property);
    SalesforcePropertiesPageActions.fillSOQLPropertyField(SOQLQueryType.valueOf(value));
  }

  @When("configure Salesforce source for an SOQL Query of type: {string}")
  public void configureSalesforceForSoqlQuery(String queryType) {
    SalesforcePropertiesPageActions.configureSalesforcePluginForSoqlQuery(SOQLQueryType.valueOf(queryType));
  }

  @When("configure Salesforce source for an SObject Query of SObject: {string}")
  public void configureSalesforceForSObjectQuery(String sObjectName) {
    SalesforcePropertiesPageActions.configureSalesforcePluginForSObjectQuery(SObjects.valueOf(sObjectName));
  }

  @When("fill SOQL Query field with a Query: {string}")
  public void fillSoqlQueryFieldWithStarQuery(String query) {
    SalesforcePropertiesPageActions.fillSOQLPropertyField(SOQLQueryType.valueOf(query));
  }

  @When("fill SObject Name property with an SObject Name: {string}")
  public void fillSObjectNameFieldWithInvalidValue(String sObjectName) {
    SalesforcePropertiesPageActions.fillSObjectName(sObjectName);
  }

  @Then("Use new connection")
  public void clickOnNewServiceNowConnection() {
    SalesforcePropertiesPageActions.clickOnServicenowConnection();
  }

  @Then("Validate the values of records transferred from Salesforce to Bigquery is equal")
  public void validateTheValuesOfRecordsTransferredFromSalesforceToBigqueryIsEqual() throws IOException,
    InterruptedException {
    boolean recordsMatched = BQValidation.validateSalesforceAndBQRecordValues(
      PluginPropertyUtils.pluginProp("sobject.Automation_custom_c"),
      PluginPropertyUtils.pluginProp("bqTargetTable")
    );
    Assert.assertTrue("Value of records transferred to the target table should be equal to the value " +
                        "of the records in the source table", recordsMatched);
  }
}

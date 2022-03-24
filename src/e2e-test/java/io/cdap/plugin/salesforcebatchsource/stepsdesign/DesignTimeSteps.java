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

package io.cdap.plugin.salesforcebatchsource.stepsdesign;

import io.cdap.e2e.pages.actions.CdfPluginPropertiesActions;
import io.cdap.plugin.salesforcebatchsource.actions.SalesforcePropertiesPageActions;
import io.cdap.plugin.utils.SchemaTable;
import io.cdap.plugin.utils.enums.SOQLQueryType;
import io.cdap.plugin.utils.enums.SObjects;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.RandomStringUtils;

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
    SalesforcePropertiesPageActions.fillSOQLPropertyField(value);
  }

  @When("configure Salesforce source for an SOQL Query of type: {string}")
  public void configureSalesforceForSoqlQuery(String queryType) {
    SalesforcePropertiesPageActions.configureSalesforcePluginForSoqlQuery(SOQLQueryType.valueOf(queryType));
  }

  @When("configure Salesforce source for an SObject Query of SObject: {string}")
  public void configureSalesforceForSObjectQuery(String sObjectName) {
    SalesforcePropertiesPageActions.configureSalesforcePluginForSObjectQuery(SObjects.valueOf(sObjectName));
  }

  @Then("verify the Output Schema table for an SOQL query of type: {string}")
  public void verifyOutputSchemaTableForSoqlQuery(String queryType) {
    SchemaTable schemaTable = SalesforcePropertiesPageActions.
      getExpectedSchemaTableForSOQLQuery(SOQLQueryType.valueOf(queryType));
    SalesforcePropertiesPageActions.verifyOutputSchemaTable(schemaTable);
  }

  @Then("verify the Output Schema table for an SObject Query of SObject: {string}")
  public void verifyOutputSchemaTableForSObjectQuery(String sObjectName) {
    SchemaTable schemaTable = SalesforcePropertiesPageActions.
      getExpectedSchemaTableForSObjectQuery(SObjects.valueOf(sObjectName));
    SalesforcePropertiesPageActions.verifyOutputSchemaTable(schemaTable);
  }

  @When("fill SOQL Query field with a Query: {string}")
  public void fillSoqlQueryFieldWithStarQuery(String query) {
    SalesforcePropertiesPageActions.fillSOQLPropertyField(SOQLQueryType.valueOf(query));
  }

  @When("fill SObject Name property with an SObject Name: {string}")
  public void fillSObjectNameFieldWithInvalidValue(String sObjectName) {
    SalesforcePropertiesPageActions.fillSObjectName(sObjectName);
  }

  @And("fill 'Last Modified After' property in format yyyy-MM-ddThh:mm:ssZ: {string}")
  public void fillLastModifiedAfter(String value) {
    SalesforcePropertiesPageActions.fillLastModifiedAfter(value);
  }

  @And("fill 'Last Modified Before' property in format yyyy-MM-ddThh:mm:ssZ: {string}")
  public void fillLastModifiedBefore(String value) {
    SalesforcePropertiesPageActions.fillLastModifiedBefore(value);
  }
}

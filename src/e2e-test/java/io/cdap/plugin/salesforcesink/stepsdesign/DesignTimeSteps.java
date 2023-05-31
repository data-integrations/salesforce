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


package io.cdap.plugin.salesforcesink.stepsdesign;

import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.salesforcesink.actions.SalesforcePropertiesPageActions;
import io.cdap.plugin.utils.enums.ErrorHandlingOptions;
import io.cdap.plugin.utils.enums.OperationTypes;
import io.cdap.plugin.utils.enums.SObjects;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Design-time steps of Salesforce plugins.
 */
public class DesignTimeSteps {

  @When("Fill SObject Name as: {string}")
  public void fillSObjectName(String sObjectName) {
    SalesforcePropertiesPageActions.fillsObjectName(sObjectName);
  }

  @And("Configure Salesforce Sink for an SObjectName: {string}")
  public void configureSalesforceSinkForSObjectName(String sObjectName) {
    SalesforcePropertiesPageActions
      .configureSalesforcePluginForSobjectName(SObjects.valueOf(sObjectName));
  }

  @And("Select Error handling as: {}")
  public void selectOptionTypeForErrorHandling(ErrorHandlingOptions option) {
    SalesforcePropertiesPageActions.selectErrorHandlingOptionType(option.value);
  }

  @Then("Select Operation type as: {string}")
  public void selectOperationType(String operationType) {
    SalesforcePropertiesPageActions.selectOperationType(OperationTypes.valueOf(operationType));
  }

  @And("Fill Max Records Per Batch as: {string}")
  public void fillMaxRecordsPerBatch(String maxRecordsPerBatch) {
    SalesforcePropertiesPageActions.fillMaxRecords(PluginPropertyUtils.pluginProp(maxRecordsPerBatch));
  }

  @And("Fill Max Bytes Per Batch as: {string}")
  public void fillMaxBytesPerBatch(String maxBytesPerBatch) {
    SalesforcePropertiesPageActions.fillMaxBytes(PluginPropertyUtils.pluginProp(maxBytesPerBatch));
  }

  @When("We are waiting")
  public void weAreWaiting() throws InterruptedException {
    TimeUnit time = TimeUnit.SECONDS;
    time.sleep(30000);
  }

  @Then("Test if sandbox is accessible")
  public void testIfSandboxIsAccessible() throws IOException {
    URL url = new URL("http://localhost:11011/pipelines/ns/default/studio");
    HttpURLConnection http = (HttpURLConnection)url.openConnection();
    BeforeActions.scenario.write("Opening URL : " + url);
    BeforeActions.scenario.write(http.getResponseCode() + " " + http.getResponseMessage());
    http.disconnect();
  }
}

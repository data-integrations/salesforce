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

package io.cdap.plugin.salesforcestreamingsource.stepsdesign;

import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfPluginPropertiesActions;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.BQValidation;
import io.cdap.plugin.salesforcestreamingsource.actions.SalesforcePropertiesPageActions;
import io.cdap.plugin.salesforcestreamingsource.locators.SalesforcePropertiesPage;
import io.cdap.plugin.utils.SalesforceClient;
import io.cdap.plugin.utils.enums.SOQLQueryType;
import io.cdap.plugin.utils.enums.SObjects;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.RandomStringUtils;
import org.json.JSONObject;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.Select;
import org.openqa.selenium.support.ui.WebDriverWait;
import scala.xml.Elem;
import stepsdesign.BeforeActions;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;

/**
 * Design-time steps of Salesforce Streaming plugins.
 */
public class DesignTimeSteps {
  static String customObject = SObjects.AUTOMATION_CUSTOM__C.value;
  @And("fill Topic Name field with a unique value")
  public void fillTopicNameWithUniqueValue() {
    SalesforcePropertiesPageActions.configureSalesforcePluginForTopicName();
  }

  @And("configure Salesforce source for an Push Topic Query of type: {string}")
  public void configureSalesforceSourceForPushTopicQuery(String pushTopicQueryType) {
    SalesforcePropertiesPageActions.configureSalesforcePluginForPushTopicQuery
      (SOQLQueryType.valueOf(pushTopicQueryType));
  }

  @When("Create a new Lead in Salesforce using REST API")
  public void createNewLeadInSalesforce() throws UnsupportedEncodingException {
    JSONObject lead = new JSONObject();
    String uniqueId = RandomStringUtils.randomAlphanumeric(10);
    lead.put("FirstName", "LFname_" + uniqueId);
    lead.put("LastName", "LLname_" + uniqueId);
    lead.put("Company", uniqueId + ".com");

    SalesforceClient.createObject(lead, "Lead");
  }

  @Then("Enter unique Topic name as a Runtime argument value for key: {string}")
  public void fillUniqueTopicNameInRuntimeArguments(String runtimeArgumentKey) {
    SalesforcePropertiesPageActions.fillUniqueTopicNameInRuntimeArguments(runtimeArgumentKey);
  }

  @Then("Update existing salesforce records")
  public void updateExistingSalesforceRecords() {
    String uniqueRecordId = SalesforceClient.queryObjectId(customObject);
    SalesforceClient.updateObject(uniqueRecordId, customObject);
  }


  @And("Create a new Custom Record in salesforce using REST API")
  public void createANewRecord() throws UnsupportedEncodingException,
    InterruptedException {
    // Waiting to create data in runtime
    TimeUnit time = TimeUnit.SECONDS;
    time.sleep(120);
    JSONObject custom = new JSONObject(PluginPropertyUtils.pluginProp("testData"));
    SalesforceClient.createObject(custom, customObject);
  }
  @Then("Validate records transferred from salesforce to big query is equal")
  public void validateRecordsTransferredFromSalesforceToBigQueryIsEqual() throws IOException,
    InterruptedException {
    // Waiting for data to reflect in target Table.
    TimeUnit time = TimeUnit.SECONDS;
    time.sleep(120);

    boolean recordsMatched = BQValidation.validateSalesforceAndBQRecordValues(
      PluginPropertyUtils.pluginProp("sobject.Automation_custom_c"),
       PluginPropertyUtils.pluginProp("bqTargetTable")
      );
    Assert.assertTrue("Value of records transferred to the target table should be equal to the value " +
                        "of the records in the source table", recordsMatched);
  }

  @And("Click on configure button")
  public void clickOnConfigureButton() {
    ElementHelper.clickOnElement(SalesforcePropertiesPage.configButton);
  }

  @And("Click on pipeline config")
  public void clickOnPipelineConfig() {
    ElementHelper.clickOnElement(SalesforcePropertiesPage.pipelineConfig);
  }

  @And("Click on batch time and select format")
  public void clickOnBatchTimeAndSelectFormat() {
    Select select = new Select(SalesforcePropertiesPage.batchTime);
    select.selectByIndex(0);
    Select selectformat = new Select(SalesforcePropertiesPage.timeSelect);
    selectformat.selectByIndex(1);
    ElementHelper.clickOnElement(SalesforcePropertiesPage.saveButton);
  }
}

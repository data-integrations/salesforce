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

package io.cdap.plugin.salesforcestreamingsource.stepsdesign;

import io.cdap.plugin.salesforcestreamingsource.actions.SalesforcePropertiesPageActions;
import io.cdap.plugin.utils.SalesforceClient;
import io.cdap.plugin.utils.enums.SOQLQueryType;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.RandomStringUtils;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;

/**
 * Design-time steps of Salesforce Streaming plugins.
 */
public class DesignTimeSteps {
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

    SalesforceClient.createLead(lead, "Lead");
  }

  @Then("Enter unique Topic name as a Runtime argument value for key: {string}")
  public void fillUniqueTopicNameInRuntimeArguments(String runtimeArgumentKey) {
    SalesforcePropertiesPageActions.fillUniqueTopicNameInRuntimeArguments(runtimeArgumentKey);
  }
}

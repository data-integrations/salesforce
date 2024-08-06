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

package io.cdap.plugin.servicenowsink.stepsdesign;

import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.servicenow.apiclient.ServiceNowAPIException;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import io.cdap.plugin.servicenowsink.actions.ServiceNowSinkPropertiesPageActions;
import io.cdap.plugin.tests.hooks.TestSetupHooks;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;

import java.io.IOException;

/**
 * ServiceNow sink - Properties page - Steps..
 */

public class DesignTimeSteps {
  public static String query;

  @And("Verify If new record created in ServiceNow application for table {string} is correct")
  public void verifyIfNewRecordCreatedInServiceNowApplicationForTableIsCorrect(String tableName)
      throws IOException, InterruptedException, ServiceNowAPIException {
    String tableValueFromPluginPropertiesFile = PluginPropertyUtils.pluginProp(tableName);

    switch (tableValueFromPluginPropertiesFile) {
      case "proc_rec_slip_item":
        query = "number=" + TestSetupHooks.receivingSlipLineRecordUniqueNumber;
        ServiceNowSinkPropertiesPageActions.verifyIfRecordCreatedInServiceNowIsCorrect(query,
            tableValueFromPluginPropertiesFile);
        break;
      case "agent_assist_recommendation":
        query = "name=" + TestSetupHooks.agentAssistRecommendationUniqueName;
        ServiceNowSinkPropertiesPageActions.verifyIfRecordCreatedInServiceNowIsCorrect(query,
            tableValueFromPluginPropertiesFile);
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + tableValueFromPluginPropertiesFile);
    }
  }

  @Then("Verify If an updated record in ServiceNow application for table {string} is correct")
  public void verifyIfAnUpdatedRecordInServiceNowApplicationForTableIsCorrect(String tableName)
      throws IOException, InterruptedException, ServiceNowAPIException {
    String tableValueFromPluginPropertiesFile = PluginPropertyUtils.pluginProp(tableName);
    query =  ServiceNowConstants.SYSTEM_ID + "=" + TestSetupHooks.systemId;
    ServiceNowSinkPropertiesPageActions.verifyIfRecordUpdatedInServiceNowIsCorrect(query,
        tableValueFromPluginPropertiesFile);
  }

  @Then("Verify error in Input Schema for non creatable fields")
  public void verifyErrorInInputSchemaForNonCreatableFields() {
    ServiceNowSinkPropertiesPageActions.verifyErrorForNonCreatableFields();
  }
}

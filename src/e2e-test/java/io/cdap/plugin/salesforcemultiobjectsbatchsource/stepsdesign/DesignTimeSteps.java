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

package io.cdap.plugin.salesforcemultiobjectsbatchsource.stepsdesign;

import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.BQValidation;
import io.cdap.plugin.salesforcemultiobjectsbatchsource.actions.SalesforceMultiObjectsPropertiesPageActions;
import io.cdap.plugin.utils.enums.SObjects;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Design-time steps of Salesforce MultiObjects Batch Source plugin.
 */
public class DesignTimeSteps {
  List<SObjects> whiteListedSObjects = new ArrayList<>();
  List<SObjects> blackListedSObjects = new ArrayList<>();

  @When("fill White List with below listed SObjects:")
  public void fillWhiteList(DataTable table) {
    List<String> list = table.asList();

    for (String sObject : list) {
      whiteListedSObjects.add(SObjects.valueOf(sObject));
    }
    SalesforceMultiObjectsPropertiesPageActions.fillWhiteListWithSObjectNames(whiteListedSObjects);
  }

  @When("fill Black List with below listed SObjects:")
  public void fillBlackList(DataTable table) {
    List<String> list = table.asList();

    for (String sObject : list) {
      blackListedSObjects.add(SObjects.valueOf(sObject));
    }
    SalesforceMultiObjectsPropertiesPageActions.fillBlackListWithSObjectNames(blackListedSObjects);
  }

  @Then("verify invalid SObject name validation message for White List")
  public void verifyInvalidSObjectNameValidationMessageForWhiteList() {
    SalesforceMultiObjectsPropertiesPageActions
      .verifyInvalidSObjectNameValidationMessageForWhiteList(whiteListedSObjects);
  }

  @Then("verify invalid SObject name validation message for Black List")
  public void verifyInvalidSObjectNameValidationMessageForBlackList() {
    SalesforceMultiObjectsPropertiesPageActions
      .verifyInvalidSObjectNameValidationMessageForBlackList(blackListedSObjects);
  }
  @Then("Validate the values of records transferred to target Big Query table is equal to the values from multi " +
    "object source table")
  public void validateTheValuesOfRecordsTransferredToTargetBigQueryTableIsEqualToTheValuesFromMultiObjectSourceTable()
    throws InterruptedException, IOException {
    boolean recordsMatched = BQValidation.validateSalesforceMultiObjectToBQRecordValues();
    Assert.assertTrue("Value of records transferred to the target table should be equal to the value " +
                        "of the records in the source table", recordsMatched);
  }
}

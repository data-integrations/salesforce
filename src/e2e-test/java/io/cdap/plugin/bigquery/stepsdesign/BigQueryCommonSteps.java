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

package io.cdap.plugin.bigquery.stepsdesign;

import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.plugin.tests.hooks.TestSetupHooks;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;

import java.io.IOException;

/**
 * Represents common steps of BigQuery plugins.
 */

public class BigQueryCommonSteps {
  @When("Configure BigQuery sink plugin for Dataset and Table")
  public void configureBqSinkPlugin() {
    String referenceName = "Test" + RandomStringUtils.randomAlphanumeric(10);
    CdfBigQueryPropertiesActions.enterBigQueryReferenceName(referenceName);
    CdfBigQueryPropertiesActions.enterBigQueryDataset(TestSetupHooks.bqTargetDataset);
    CdfBigQueryPropertiesActions.enterBigQueryTable(TestSetupHooks.bqTargetTable);
  }

  @When("Configure BigQuery Multi Table sink plugin for Dataset")
  public void configureBqMultiTableSinkPlugin() {
    String referenceName = "Test" + RandomStringUtils.randomAlphanumeric(10);
    CdfBigQueryPropertiesActions.enterBigQueryReferenceName(referenceName);
    CdfBigQueryPropertiesActions.enterBigQueryDataset(TestSetupHooks.bqTargetDataset);
  }

  @Then("Verify count of no of records transferred to the target BigQuery Table")
  public void getCountOfNoOfRecordsTransferredToTargetBigQueryTable() throws IOException, InterruptedException {
    int countRecords = BigQueryClient.countBqQuery(TestSetupHooks.bqTargetDataset, TestSetupHooks.bqTargetTable);
    Assert.assertEquals("Number of records transferred to BigQuery should be equal to " +
        "records out count displayed on the Source plugin: ",
      countRecords, CdfPipelineRunAction.getCountDisplayedOnSourcePluginAsRecordsOut());
    Assert.assertEquals("Number of records transferred to BigQuery should be equal to " +
        "records in count displayed on the Sink plugin: ",
      countRecords, CdfPipelineRunAction.getCountDisplayedOnSinkPluginAsRecordsIn());
  }
}

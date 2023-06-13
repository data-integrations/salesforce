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


package io.cdap.plugin.salesforcesink.stepsdesign;

import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.BQValidation;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

/**
 * Design-time steps of Salesforce plugins.
 */
public class DesignTimeSteps {

  @Then("Validate the values of records transferred to target Salesforce is equal to the values from source " +
    "BigQuery table")
  public void validateTheValuesOfRecordsTransferredToTargetSalesforceIsEqualToTheValuesFromSourceBigQueryTable()
    throws InterruptedException, IOException {
    boolean recordsMatched = BQValidation.validateSalesforceAndBQRecordValues(
      PluginPropertyUtils.pluginProp("sobject.Automation_custom_c"),
      PluginPropertyUtils.pluginProp("bqSourceTable")
    );
    Assert.assertTrue("Value of records transferred to the target table should be equal to the value " +
                        "of the records in the source table", recordsMatched);
  }
}

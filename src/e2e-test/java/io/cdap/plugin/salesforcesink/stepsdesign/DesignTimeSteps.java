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
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceWideRecordReader;
import io.cdap.plugin.salesforcesink.actions.SalesforcePropertiesPageActions;
import io.cdap.plugin.utils.enums.ErrorHandlingOptions;
import io.cdap.plugin.utils.enums.OperationTypes;
import io.cdap.plugin.utils.enums.SObjects;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;

/**
 * Design-time steps of Salesforce plugins.
 */
public class DesignTimeSteps {

String invalidMaxRecords = "1000000";
String invalidMaxBytes = "1000000000";
String invalidsObjectName = "abcdef";

    @And("configure Salesforce sink for an SobjectName: {string}")
    public void configureSalesforceSinkForSobjectName(String sObjectName) {
        SalesforcePropertiesPageActions
                .configureSalesforcePluginForSobjectName(SObjects.valueOf(sObjectName));
    }

    @And("then Select option type for error handling as {}")
    public void selectOptionTypeForErrorHandling(ErrorHandlingOptions option) {
        SalesforcePropertiesPageActions.selectErrorHandlingOptionType(option.value);
    }

    @Then("then select operation type as {string}")
    public void selectOperationType(String operationType) {
        SalesforcePropertiesPageActions.selectOperationType(OperationTypes.valueOf(operationType));

    }

    @And("fill max Records Per Batch as: {string}")
    public void fillMaxRecordsPerBatch(String maxRecordsPerBatch) {
        SalesforcePropertiesPageActions.fillMaxRecords(PluginPropertyUtils.pluginProp(maxRecordsPerBatch));
    }

    @And("fill max Bytes Per Batch as: {string}")
    public void fillMaxBytesPerBatch(String maxBytesPerBatch) {
        SalesforcePropertiesPageActions.fillMaxBytes(maxBytesPerBatch);
    }

    @And("fill SObject Name property in Sink with an invalid value")
    public void fillSObjectNamePropertyInSinkWithAnInvalidValue() {
        SalesforcePropertiesPageActions.fillsObjectName(invalidsObjectName);
    }
}

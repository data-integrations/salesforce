# Copyright © 2023 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

@SalesforceSalesCloud
@SFMultiObjectsBatchSource
@Smoke
@Regression

Feature: Salesforce Multi Objects Batch Source - Run time Scenarios

  @MULTIBATCH-TS-SF-RNTM-01 @BQ_SINK_MULTI_TEST @CREATE_TEST_DATA @CREATE_TEST_DATA2 @DELETE_TEST_DATA @DELETE_TEST_DATA2
  Scenario: Verify user should be able to preview, deploy and run pipeline for valid White List
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SalesforceMultiObjects"
    And fill Authentication properties for Salesforce Admin user
    And fill Reference Name property
    And fill White List with below listed SObjects:
      | AUTOMATION_CUSTOM__C | AUTOMATION_CUSTOM2__C |
    And Enter input plugin property: "datetimeAfter" with value: "data.modified.after"
    Then Validate "Salesforce" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryMultiTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery Multi Table"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    And Connect plugins: "SalesforceMultiObjects" and "BigQuery Multi Table" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target Big Query table is equal to the values from multi object source table

  @MULTIBATCH-TS-SF-RNTM-02 @BQ_SINK_MULTI_TEST @CREATE_TEST_DATA @CREATE_TEST_DATA2 @DELETE_TEST_DATA @DELETE_TEST_DATA2
  Scenario: Verify user should be able to preview, deploy and run pipeline for valid Black List
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SalesforceMultiObjects"
    And fill Authentication properties for Salesforce Admin user
    And fill Reference Name property
    And fill White List with below listed SObjects:
      | AUTOMATION_CUSTOM__C | AUTOMATION_CUSTOM2__C |
    And fill Black List with below listed SObjects:
      | ACCOUNT | CONTACT |
    And Enter input plugin property: "datetimeAfter" with value: "last.modified.after"
    Then Validate "Salesforce" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryMultiTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery Multi Table"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect plugins: "SalesforceMultiObjects" and "BigQuery Multi Table" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target Big Query table is equal to the values from multi object source table


  @MULTIBATCH-TS-SF-RNTM-03 @CONNECTION @BQ_SINK_MULTI_TEST @CREATE_TEST_DATA @CREATE_TEST_DATA2 @DELETE_TEST_DATA @DELETE_TEST_DATA2
  Scenario: Verify user should be able to deploy and run the pipeline using connection manager functionality
    When Open Datafusion Project to configure pipeline
    And Select plugin: "Salesforce Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SalesforceMultiObjects"
    And Enter input plugin property: "referenceName" with value: "referenceName"
    And Click plugin property: "switch-useConnection"
    And Click on the Browse Connections button
    And Click on the Add Connection button
    And Click plugin property: "connector-Salesforce"
    And Enter input plugin property: "name" with value: "connection.name"
    And fill Authentication properties for Salesforce Admin user
    Then Click on the Test Connection button
    And Verify the test connection is successful
    Then Click on the Create button
    Then Use new connection
    And fill White List with below listed SObjects:
      | AUTOMATION_CUSTOM__C | AUTOMATION_CUSTOM2__C |
    And Enter input plugin property: "datetimeAfter" with value: "data.modified.after"
    Then Validate "Salesforce Multi Objects" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryMultiTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery Multi Table"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    And Connect plugins: "SalesforceMultiObjects" and "BigQuery Multi Table" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate the values of records transferred to target Big Query table is equal to the values from multi object source table

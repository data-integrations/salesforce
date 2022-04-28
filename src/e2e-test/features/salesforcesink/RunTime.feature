# Copyright © 2022 Cask Data, Inc.
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
@SFSink
@Smoke
@Regression
Feature: Salesforce Sink - Run time Scenarios

  @SINK-TS-SF-RNTM-01 @BQ_SOURCE_TEST
  Scenario: Verify user should be able to preview and run pipeline with Salesforce Sink plugin
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery source plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "Salesforce" from the plugins list
    And Connect source as "BigQuery" and sink as "Salesforce" to establish connection
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And Configure Salesforce Sink for an SObjectName: "Lead"
    And Select Operation type as: "INSERT"
    And Fill Max Records Per Batch as: "ten.thousand.records"
    And Fill Max Bytes Per Batch as: "ten.million.records"
    And Select Error handling as: SKIP_ON_ERROR
    Then Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Preview and run the pipeline
    And Verify the preview of pipeline is "successfully"

  @SINK-TS-SF-RNTM-02 @BQ_SOURCE_TEST
  Scenario: Verify user should be able to deploy and run pipeline with Salesforce Sink plugin
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery source plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "Salesforce" from the plugins list
    And Connect source as "BigQuery" and sink as "Salesforce" to establish connection
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And Configure Salesforce Sink for an SObjectName: "Lead"
    And Select Operation type as: "INSERT"
    And Fill Max Records Per Batch as: "ten.thousand.records"
    And Fill Max Bytes Per Batch as: "ten.million.records"
    And Select Error handling as: SKIP_ON_ERROR
    Then Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"

  @SINK-TS-SF-RNTM-03 @BQ_SOURCE_TEST
  Scenario: Verify user should be able to deploy and run pipeline when Error Handling is selected as 'Stop on error'
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery source plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "Salesforce" from the plugins list
    And Connect source as "BigQuery" and sink as "Salesforce" to establish connection
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And Configure Salesforce Sink for an SObjectName: "Lead"
    And Select Operation type as: "INSERT"
    And Fill Max Records Per Batch as: "ten.thousand.records"
    And Fill Max Bytes Per Batch as: "ten.million.records"
    And Select Error handling as: STOP_ON_ERROR
    Then Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"

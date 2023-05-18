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
@SFStreamingSource
@Smoke
@Regression
Feature: Salesforce Streaming Source - Run time Scenarios

  @STREAMING-TS-SF-RNTM-01 @BQ_SINK_TEST
  Scenario: Verify user should be able to deploy and run pipeline for Streaming Source using SObjectName
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Realtime"
    And Select Realtime Source plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Topic Name field with a unique value
    And fill Authentication properties for Salesforce Admin user
    And Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
#    And Configure BigQuery sink plugin for Dataset and Table
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click plugin property: "truncateTable"
    Then Click plugin property: "updateTableSchema"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect plugins: "Salesforce" and "BigQuery" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Create a new Lead in Salesforce using REST API
    And Wait till pipeline is in running status with a timeout of 120 seconds
    And Stop the pipeline
    And Open and capture logs
    And Verify the pipeline status is "Stopped"

  @STREAMING-TS-SF-RNTM-02 @BQ_SINK_TEST
  Scenario: Verify user should be able to deploy and run pipeline for Streaming Source using SOQL Query
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Realtime"
    And Select Realtime Source plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Topic Name field with a unique value
    And fill Authentication properties for Salesforce Admin user
    And Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
#    And Configure BigQuery sink plugin for Dataset and Table
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click plugin property: "truncateTable"
    Then Click plugin property: "updateTableSchema"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect plugins: "Salesforce" and "BigQuery" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Create a new Lead in Salesforce using REST API
    And Wait till pipeline is in running status with a timeout of 120 seconds
    And Stop the pipeline
    And Open and capture logs
    And Verify the pipeline status is "Stopped"

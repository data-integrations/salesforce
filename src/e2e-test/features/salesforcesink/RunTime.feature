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
@SinkAndStre
Feature: Salesforce Sink - Run time Scenarios

  @SINK-TS-SF-RNTM-01 @BQ_SOURCE_TEST
  Scenario: Verify user should be able to preview, deploy and run pipeline with Salesforce Sink plugin
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
#    And Configure BigQuery source plugin for Dataset and Table
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "Salesforce" from the plugins list
    And Connect plugins: "BigQuery" and "Salesforce" to establish connection
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And Configure Salesforce Sink for an SObjectName: "LEAD"
    And Select Operation type as: "INSERT"
    And Fill Max Records Per Batch as: "ten.thousand.records"
    And Fill Max Bytes Per Batch as: "ten.million.records"
    And Select Error handling as: SKIP_ON_ERROR
    Then Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Preview and run the pipeline
#    And Verify the preview of pipeline is "successfully"
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

#  @SINK-TS-SF-RNTM-02 @BQ_SOURCE_TEST
#  Scenario: Verify user should be able to deploy and run pipeline with Salesforce Sink plugin
#    When Open Datafusion Project to configure pipeline
#    And Select plugin: "BigQuery" from the plugins list as: "Source"
#    And Navigate to the properties page of plugin: "BigQuery"
#    And Configure BigQuery source plugin for Dataset and Table
#    Then Validate "BigQuery" plugin properties
#    And Close the Plugin Properties page
#    And Select Sink plugin: "Salesforce" from the plugins list
#    And Connect source as "BigQuery" and sink as "Salesforce" to establish connection
#    And Navigate to the properties page of plugin: "Salesforce"
#    And fill Authentication properties for Salesforce Admin user
#    And Configure Salesforce Sink for an SObjectName: "Lead"
#    And Select Operation type as: "INSERT"
#    And Fill Max Records Per Batch as: "ten.thousand.records"
#    And Fill Max Bytes Per Batch as: "ten.million.records"
#    And Select Error handling as: SKIP_ON_ERROR
#    Then Validate "Salesforce" plugin properties
#    And Close the Plugin Properties page
#    And Save the pipeline
#    And Save and Deploy Pipeline
#    And Run the Pipeline in Runtime
#    And Wait till pipeline is in running state
#    And Open and capture logs
#    And Verify the pipeline status is "Succeeded"

  @SINK-TS-SF-RNTM-03 @BQ_SOURCE_TEST
  Scenario: Verify user should be able to preview, deploy and run pipeline when Error Handling is selected as 'Stop on error'
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
#    And Configure BigQuery source plugin for Dataset and Table
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "Salesforce" from the plugins list
    And Connect plugins: "BigQuery" and "Salesforce" to establish connection
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And Configure Salesforce Sink for an SObjectName: "LEAD"
    And Select Operation type as: "INSERT"
    And Fill Max Records Per Batch as: "ten.thousand.records"
    And Fill Max Bytes Per Batch as: "ten.million.records"
    And Select Error handling as: STOP_ON_ERROR
    Then Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
#    And Save and Deploy Pipeline
#    And Run the Pipeline in Runtime
#    And Wait till pipeline is in running state
#    And Open and capture logs
#    And Verify the pipeline status is "Succeeded"
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

  @SINK-TS-SF-RNTM-04 @BQ_SOURCE_TEST @CONNECTION
  Scenario: Verify user should be able to deploy and run the pipeline using connection manager functionality
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "Salesforce" from the plugins list
    And Connect plugins: "BigQuery" and "Salesforce" to establish connection
    And Navigate to the properties page of plugin: "Salesforce"
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
    And Configure Salesforce Sink for an SObjectName: "LEAD"
    And Select Operation type as: "INSERT"
    And Fill Max Records Per Batch as: "ten.thousand.records"
    And Fill Max Bytes Per Batch as: "ten.million.records"
    And Select Error handling as: SKIP_ON_ERROR
    Then Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
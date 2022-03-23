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
@SFBatchSource
@Smoke
@Regression
Feature: Salesforce Batch Source - Run time Scenarios


  @BATCH-TS-SF-RNTM-01 @BQ_SINK_TEST
  Scenario Outline: Verify user should be able to preview the pipeline where Salesforce is configured for valid SObject Name
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And configure Salesforce source for an SObject Query of SObject: "<SObjectName>"
    And fill Last modified After in format: yyyy-MM-ddThh:mm:ssZ: "lastmodified.after"
    Then Validate "Salesforce" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties Page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties Page
    And Connect source as "Salesforce" and sink as "BigQueryTable" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    And Verify the preview of pipeline is "success"
    And Verify sink plugin's Preview Data for Input Records table and the Input Schema matches the Output Schema of Source plugin
    Examples:
      | SObjectName |
      | LEAD        |



  @BATCH-TS-SF-RNTM-02 @BQ_SINK_TEST
  Scenario Outline:Verify user should be able to deploy and run the pipeline where Salesforce is configured for SObject Name
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And configure Salesforce source for an SObject Query of SObject: "<SObjectName>"
    And fill Last modified After in format: yyyy-MM-ddThh:mm:ssZ: "lastmodified.after"
    Then Validate "Salesforce" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties Page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties Page
    And Connect source as "Salesforce" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Examples:
      | SObjectName |
      | LEAD        |



     @BATCH-TS-SF-RNTM-03 @BQ_SINK_TEST
  Scenario Outline: Verify user should be able to preview the pipeline where Salesforce is configured for valid SOQL Query
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And configure Salesforce source for an SOQL Query of type: "<QueryType>"
    Then Validate "Salesforce" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties Page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties Page
    And Connect source as "Salesforce" and sink as "BigQueryTable" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    And Verify the preview of pipeline is "success"
    And Verify sink plugin's Preview Data for Input Records table and the Input Schema matches the Output Schema of Source plugin
    Examples:
      | QueryType     |
      | WHERE         |


  @BATCH-TS-SF-RNTM-04 @BQ_SINK_TEST
  Scenario Outline:Verify user should be able to deploy and run the pipeline where Salesforce is configured for valid SOQL Query
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And configure Salesforce source for an SOQL Query of type: "<QueryType>"
    Then Validate "Salesforce" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties Page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties Page
    And Connect source as "Salesforce" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Examples:
      | QueryType     |
      | WHERE         |

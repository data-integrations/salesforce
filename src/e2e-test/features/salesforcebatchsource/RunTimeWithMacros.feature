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
@Regression
Feature: Salesforce Batch Source - Run time Scenarios (macro)

  @BATCH-TS-SF-RNTM-MACRO-01 @BQ_SINK_TEST
  Scenario:Verify user should be able to preview the pipeline when plugin is configured for SObject Name with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And Click on the Macro button of Property: "username" and set the value to: "Username"
    And Click on the Macro button of Property: "password" and set the value to: "Password"
    And Click on the Macro button of Property: "securityToken" and set the value to: "SecurityToken"
    And Click on the Macro button of Property: "consumerKey" and set the value to: "ConsumerKey"
    And Click on the Macro button of Property: "consumerSecret" and set the value to: "ConsumerSecret"
    And Click on the Macro button of Property: "loginUrl" and set the value to: "LoginUrl"
    And Click on the Macro button of Property: "sObjectName" and set the value to: "SObjectName"
    And fill 'Last Modified After' property in format yyyy-MM-ddThh:mm:ssZ: "last.modified.after"
    And Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce" and sink as "BigQueryTable" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    And Enter runtime argument value "admin.username" for key "Username"
    And Enter runtime argument value "admin.password" for key "Password"
    And Enter runtime argument value "admin.security.token" for key "SecurityToken"
    And Enter runtime argument value "admin.consumer.key" for key "ConsumerKey"
    And Enter runtime argument value "admin.consumer.secret" for key "ConsumerSecret"
    And Enter runtime argument value "login.url" for key "LoginUrl"
    And Enter runtime argument value "Salesforce.sobjectName" for key "SObjectName"
    And Run the preview of pipeline with runtime arguments
    And Verify the preview of pipeline is "successfully"

  @BATCH-TS-SF-RNTM-MACRO-02 @BQ_SINK_TEST
  Scenario:Verify user should be able to deploy and run the pipeline when plugin is configured for SObject Name with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And Click on the Macro button of Property: "username" and set the value to: "Username"
    And Click on the Macro button of Property: "password" and set the value to: "Password"
    And Click on the Macro button of Property: "securityToken" and set the value to: "SecurityToken"
    And Click on the Macro button of Property: "consumerKey" and set the value to: "ConsumerKey"
    And Click on the Macro button of Property: "consumerSecret" and set the value to: "ConsumerSecret"
    And Click on the Macro button of Property: "loginUrl" and set the value to: "LoginUrl"
    And Click on the Macro button of Property: "sObjectName" and set the value to: "SObjectName"
    And fill 'Last Modified After' property in format yyyy-MM-ddThh:mm:ssZ: "last.modified.after"
    And Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "admin.username" for key "Username"
    And Enter runtime argument value "admin.password" for key "Password"
    And Enter runtime argument value "admin.security.token" for key "SecurityToken"
    And Enter runtime argument value "admin.consumer.key" for key "ConsumerKey"
    And Enter runtime argument value "admin.consumer.secret" for key "ConsumerSecret"
    And Enter runtime argument value "login.url" for key "LoginUrl"
    And Enter runtime argument value "Salesforce.sobjectName" for key "SObjectName"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"

  @BATCH-TS-SF-RNTM-MACRO-03 @BQ_SINK_TEST
  Scenario: Verify user should be able to preview the pipeline when plugin is configured for SOQL Query with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And Click on the Macro button of Property: "username" and set the value to: "Username"
    And Click on the Macro button of Property: "password" and set the value to: "Password"
    And Click on the Macro button of Property: "securityToken" and set the value to: "SecurityToken"
    And Click on the Macro button of Property: "consumerKey" and set the value to: "ConsumerKey"
    And Click on the Macro button of Property: "consumerSecret" and set the value to: "ConsumerSecret"
    And Click on the Macro button of Property: "loginUrl" and set the value to: "LoginUrl"
    And Click on the Macro button of Property: "query" and set the value to: "Query"
    And Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce" and sink as "BigQueryTable" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    And Enter runtime argument value "admin.username" for key "Username"
    And Enter runtime argument value "admin.password" for key "Password"
    And Enter runtime argument value "admin.security.token" for key "SecurityToken"
    And Enter runtime argument value "admin.consumer.key" for key "ConsumerKey"
    And Enter runtime argument value "admin.consumer.secret" for key "ConsumerSecret"
    And Enter runtime argument value "login.url" for key "LoginUrl"
    And Enter runtime argument value "where.query" for key "Query"
    And Run the preview of pipeline with runtime arguments
    And Verify the preview of pipeline is "successfully"

  @BATCH-TS-SF-RNTM-MACRO-04 @BQ_SINK_TEST
  Scenario: Verify user should be able to deploy and run the pipeline when plugin is configured for SOQL Query with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And Click on the Macro button of Property: "username" and set the value to: "Username"
    And Click on the Macro button of Property: "password" and set the value to: "Password"
    And Click on the Macro button of Property: "securityToken" and set the value to: "SecurityToken"
    And Click on the Macro button of Property: "consumerKey" and set the value to: "ConsumerKey"
    And Click on the Macro button of Property: "consumerSecret" and set the value to: "ConsumerSecret"
    And Click on the Macro button of Property: "loginUrl" and set the value to: "LoginUrl"
    And Click on the Macro button of Property: "query" and set the value to: "Query"
    And Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "Salesforce" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "admin.username" for key "Username"
    And Enter runtime argument value "admin.password" for key "Password"
    And Enter runtime argument value "admin.security.token" for key "SecurityToken"
    And Enter runtime argument value "admin.consumer.key" for key "ConsumerKey"
    And Enter runtime argument value "admin.consumer.secret" for key "ConsumerSecret"
    And Enter runtime argument value "login.url" for key "LoginUrl"
    And Enter runtime argument value "where.query" for key "Query"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"

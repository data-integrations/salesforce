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
@SFStreamingSource
@Smoke
@Regression

Feature: Salesforce Streaming Source - Run time Scenarios (macros)

  @STREAMING-TS-SF-RNTM-MACRO-01 @BQ_SINK_TEST @DELETE_PUSH_TOPIC @DELETE_TEST_DATA
  Scenario: Verify user should be able to deploy and run pipeline with macro for Streaming Source using SObjectName
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Realtime"
    And Select Realtime Source plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And Click on the Macro button of Property: "username" and set the value to: "Username"
    And Click on the Macro button of Property: "password" and set the value to: "Password"
    And Click on the Macro button of Property: "securityToken" and set the value to: "SecurityToken"
    And Click on the Macro button of Property: "consumerKey" and set the value to: "ConsumerKey"
    And Click on the Macro button of Property: "consumerSecret" and set the value to: "ConsumerSecret"
    And Click on the Macro button of Property: "loginUrl" and set the value to: "LoginUrl"
    And Click on the Macro button of Property: "pushTopicName" and set the value to: "TopicName"
    And Click on the Macro button of Property: "sObjectName" and set the value to: "SObjectName"
    And Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
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
    And Click on configure button
    And Click on pipeline config
    And Click on batch time and select format
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value from environment variable "admin.username" for key "Username"
    And Enter runtime argument value from environment variable "admin.password" for key "Password"
    And Enter runtime argument value from environment variable "admin.security.token" for key "SecurityToken"
    And Enter runtime argument value from environment variable "admin.consumer.key" for key "ConsumerKey"
    And Enter runtime argument value from environment variable "admin.consumer.secret" for key "ConsumerSecret"
    And Enter runtime argument value "login.url" for key "LoginUrl"
    And Enter unique Topic name as a Runtime argument value for key: "TopicName"
    And Enter runtime argument value "sobject.Automation_custom_c" for key "SObjectName"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait for pipeline to be in status: "Running" with a timeout of 240 seconds
    And Create a new Custom Record in salesforce using REST API
    Then Validate records transferred from salesforce to big query in runtime is equal
    Then Update existing salesforce records
    Then Validate records transferred from salesforce to big query in runtime is equal
    And Stop the pipeline
    And Open and capture logs
    Then Verify the pipeline status is "Stopped"

  @STREAMING-TS-SF-RNTM-MACRO-02 @BQ_SINK_TEST @DELETE_PUSH_TOPIC @DELETE_TEST_DATA
  Scenario: Verify user should be able to deploy and run pipeline with macro for Streaming Source using SOQL Query
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Realtime"
    And Select Realtime Source plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And Click on the Macro button of Property: "username" and set the value to: "Username"
    And Click on the Macro button of Property: "password" and set the value to: "Password"
    And Click on the Macro button of Property: "securityToken" and set the value to: "SecurityToken"
    And Click on the Macro button of Property: "consumerKey" and set the value to: "ConsumerKey"
    And Click on the Macro button of Property: "consumerSecret" and set the value to: "ConsumerSecret"
    And Click on the Macro button of Property: "loginUrl" and set the value to: "LoginUrl"
    And Click on the Macro button of Property: "pushTopicName" and set the value to: "TopicName"
    And Click on the Macro button of Property: "pushTopicQuery" and set the value to: "Query"
    And Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
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
    And Click on configure button
    And Click on pipeline config
    And Click on batch time and select format
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value from environment variable "admin.username" for key "Username"
    And Enter runtime argument value from environment variable "admin.password" for key "Password"
    And Enter runtime argument value from environment variable "admin.security.token" for key "SecurityToken"
    And Enter runtime argument value from environment variable "admin.consumer.key" for key "ConsumerKey"
    And Enter runtime argument value from environment variable "admin.consumer.secret" for key "ConsumerSecret"
    And Enter runtime argument value "login.url" for key "LoginUrl"
    And Enter unique Topic name as a Runtime argument value for key: "TopicName"
    And Enter runtime argument value "test.query" for key "Query"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait for pipeline to be in status: "Running" with a timeout of 240 seconds
    And Create a new Custom Record in salesforce using REST API
    Then Validate records transferred from salesforce to big query in runtime is equal
    Then Update existing salesforce records
    Then Validate records transferred from salesforce to big query in runtime is equal
    And Stop the pipeline
    And Open and capture logs
    Then Verify the pipeline status is "Stopped"

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

Feature: Salesforce Sink - Run time Scenarios with Macro

  @SINK-TS-SF-RNTM-MACRO-01 @BQ_SINK_TEST
  Scenario Outline:Verify user should be able to preview and run pipeline using macro for Sink
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "Salesforce" from the plugins list
    And Connect source as "BigQuery" and sink as "Salesforce" to establish connection
    And Navigate to the properties page of plugin: "Salesforce"
    And Click on the Macro button of Property: "username" and set the value to: "Username"
    And Click on the Macro button of Property: "password" and set the value to: "Password"
    And Click on the Macro button of Property: "securityToken" and set the value to: "SecurityToken"
    And Click on the Macro button of Property: "consumerKey" and set the value to: "ConsumerKey"
    And Click on the Macro button of Property: "consumerSecret" and set the value to: "ConsumerSecret"
    And Click on the Macro button of Property: "loginUrl" and set the value to: "LoginUrl"
    And Click on the Macro button of Property: "sObjectName" and set the value to: "SObjectName"
    And Select Operation type as: "<OperationType>"
    And Fill Max Records Per Batch as: "TenthousandRecords"
    And Fill Max Bytes Per Batch as: "OneCroreRecords"
    And Select Error handling as: SKIP_ON_ERROR
    Then Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
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
    Examples:
      | OperationType |
      | INSERT        |
      | UPDATE        |
      | UPSERT        |

  @SINK-TS-SF-RNTM-MACRO-02 @BQ_SINK_TEST
  Scenario Outline:Verify user should be able to deploy and run pipeline using macro for Sink
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "Salesforce" from the plugins list
    And Connect source as "BigQuery" and sink as "Salesforce" to establish connection
    And Navigate to the properties page of plugin: "Salesforce"
    And Click on the Macro button of Property: "username" and set the value to: "Username"
    And Click on the Macro button of Property: "password" and set the value to: "Password"
    And Click on the Macro button of Property: "securityToken" and set the value to: "SecurityToken"
    And Click on the Macro button of Property: "consumerKey" and set the value to: "ConsumerKey"
    And Click on the Macro button of Property: "consumerSecret" and set the value to: "ConsumerSecret"
    And Click on the Macro button of Property: "loginUrl" and set the value to: "LoginUrl"
    And Click on the Macro button of Property: "sObjectName" and set the value to: "SObjectName"
    And Select Operation type as: "<OperationType>"
    And Fill Max Records Per Batch as: "TenthousandRecords"
    And Fill Max Bytes Per Batch as: "OneCroreRecords"
    And Select Error handling as: SKIP_ON_ERROR
    Then Validate "Salesforce" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
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
    Examples:
      | OperationType |
      | INSERT        |
      | UPDATE        |
      | UPSERT        |

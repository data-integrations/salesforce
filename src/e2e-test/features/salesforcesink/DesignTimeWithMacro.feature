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
Feature: Salesforce Sink - Design time Scenarios (macro)

  @SINK-TS-SF-RNTM-MACRO-01 @BQ_SOURCE_TEST
  Scenario:Verify validation message when using Macro for Sink
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "Salesforce" from the plugins list
    Then Connect plugins: "BigQuery" and "Salesforce" to establish connection
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And Click on the Macro button of Property: "username" and set the value to: "Username"
    And Click on the Macro button of Property: "password" and set the value to: "Password"
    And Click on the Macro button of Property: "securityToken" and set the value to: "SecurityToken"
    And Click on the Macro button of Property: "consumerKey" and set the value to: "ConsumerKey"
    And Click on the Macro button of Property: "consumerSecret" and set the value to: "ConsumerSecret"
    And Click on the Macro button of Property: "loginUrl" and set the value to: "LoginUrl"
    And Click on the Macro button of Property: "sObject" and set the value to: "SObjectName"
    And Click on the Macro button of Property: "operation" and set the value to: "Operation Type"
    And Click on the Macro button of Property: "maxRecordsPerBatch" and set the value to: "maxRecordsPerBatch"
    And Click on the Macro button of Property: "maxBytesPerBatch" and set the value to: "maxBytesPerBatch"
    And Click on the Macro button of Property: "errorHandling" and set the value to: "errorHandling"
    Then Validate "Salesforce" plugin properties

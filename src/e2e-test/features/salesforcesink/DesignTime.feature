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
Feature: Salesforce Sink  - Design time scenarios

  @SINK-TS-SF-DSGN-01 @BQ_SOURCE_TEST
  Scenario Outline: Verify user should be able to successfully validate the sink for valid SObjectName
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
    And Select Operation type as: "<OperationType>"
    And fill Authentication properties for Salesforce Admin user
    And Fill Max Records Per Batch as: "ten.thousand.records"
    And Fill Max Bytes Per Batch as: "ten.million.records"
    And Configure Salesforce Sink for an SObjectName: "<SObjectName>"
    And Select Error handling as: SKIP_ON_ERROR
    Then Validate "Salesforce" plugin properties
    Examples:
      | SObjectName |  OperationType  |
      | ACCOUNT     |  INSERT         |
      | ACCOUNT     |  UPDATE         |
      | ACCOUNT     |  UPSERT         |

  @SINK-TS-SF-DSGN-02 @BQ_SOURCE_TEST @CONNECTION
  Scenario: Verify user should be able to create the valid connection using connection manager functionality
    When Open Datafusion Project to configure pipeline
    And Select Sink plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And Click plugin property: "switch-useConnection"
    And Click on the Browse Connections button
    And Click on the Add Connection button
    And Click plugin property: "connector-Salesforce"
    And Enter input plugin property: "name" with value: "connection.name"
    And fill Authentication properties for Salesforce Admin user
    Then Click on the Test Connection button
    And Verify the test connection is successful

  @Debug
  Scenario: Verify if sandbox is accessible
    Given We are waiting
    Then Test if sandbox is accessible
    Then Open Datafusion Project to configure pipeline

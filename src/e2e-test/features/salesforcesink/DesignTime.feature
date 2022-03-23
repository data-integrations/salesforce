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
Feature: Salesforce Sink  - Design time scenarios

  @SINK-TS-SF-DSGN-05
  Scenario Outline: Verify user should be able to successfully validate the sink for valid SObjectName
    When Open Datafusion Project to configure pipeline
    And Select plugin: "BigQuery" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "BigQuery"
    And fill Reference Name property
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties Page
    And Select Sink plugin: "Salesforce" from the plugins list
    And Connect source as "BigQuery" and sink as "Salesforce" to establish connection
    And Navigate to the properties page of plugin: "Salesforce"
    And then select operation type as "<OperationType>"
    And fill Authentication properties for Salesforce Admin user
    And fill max Records Per Batch as: "TenthousandRecords"
    And fill max Bytes Per Batch as: "OneCroreRecords"
    And configure Salesforce sink for an SobjectName: "<SObjectName>"
    And then Select option type for error handling as SKIP_ON_ERROR
    Then Validate "Salesforce" plugin properties
    Examples:
      | SObjectName |  OperationType  |
      | ACCOUNT     |  INSERT         |
      | ACCOUNT     |  UPDATE         |
      | ACCOUNT     |  UPSERT         |




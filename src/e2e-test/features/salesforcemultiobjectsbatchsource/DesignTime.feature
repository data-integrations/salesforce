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
@SFMultiObjectsBatchSource
@Smoke
@Regression

Feature: Salesforce Multi Objects Batch Source - Design time Scenarios

  @MULTIBATCH-TS-SF-DSGN-01
  Scenario: Verify user should be able to successfully validate the source for valid SObject names in the White List
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SalesforceMultiObjects"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And fill White List with below listed SObjects:
      | ACCOUNT | CONTACT |
    Then Validate "SalesforceMultiObjects" plugin properties

  @MULTIBATCH-TS-SF-DSGN-02
  Scenario: Verify user should be able to successfully validate the source for valid SObject names in the Black List
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SalesforceMultiObjects"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And fill Black List with below listed SObjects:
      | ACCOUNT | CONTACT |
    Then Validate "SalesforceMultiObjects" plugin properties

  @MULTIBATCH-TS-SF-DSGN-03
  Scenario: Verify user should be able to successfully validate the source with Incremental Load Properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SalesforceMultiObjects"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And fill White List with below listed SObjects:
      | ACCOUNT | CONTACT |
    And Enter input plugin property: "datetimeAfter" with value: "last.modified.after"
    And Enter input plugin property: "datetimeBefore" with value: "last.modified.before"
    Then Validate "SalesforceMultiObjects" plugin properties

  @MULTIBATCH-TS-SF-DSGN-04 @CONNECTION
  Scenario: Verify user should be able to create the valid connection using connection manager functionality
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SalesforceMultiObjects"
    And Click plugin property: "switch-useConnection"
    And Click on the Browse Connections button
    And Click on the Add Connection button
    And Click plugin property: "connector-Salesforce"
    And Enter input plugin property: "name" with value: "connection.name"
    And fill Authentication properties for Salesforce Admin user
    Then Click on the Test Connection button
    And Verify the test connection is successful
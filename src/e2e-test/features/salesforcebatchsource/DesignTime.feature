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
@SourceAndSink
Feature: Salesforce Batch Source - Design time Scenarios

  @BATCH-TS-SF-DSGN-01
  Scenario Outline: Verify user should be able to get output schema for a valid SOQL query
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And configure Salesforce source for an SOQL Query of type: "<QueryType>"
    And Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "<ExpectedSchema>"
    Examples:
      | QueryType     | ExpectedSchema                    |
      | SIMPLE        | simple.query.schema               |
      | CHILDTOPARENT | ChildToParent.query.schema        |

  @BATCH-TS-SF-DSGN-02
  Scenario Outline: Verify user should be able to get output schema for a valid SObject Name
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And configure Salesforce source for an SObject Query of SObject: "<SObjectName>"
    Then Validate "Salesforce" plugin properties
    And Verify the Output Schema matches the Expected Schema: "<ExpectedScehma>"
    Examples:
      | SObjectName | ExpectedScehma |
      | LEAD        | lead.schema    |
      | ACCOUNT     | account.schema |

  @BATCH-TS-SF-DSGN-03
  Scenario: Verify user should be able to get output schema when plugin is configured with Last Modified After property
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And configure Salesforce source for an SObject Query of SObject: "ACCOUNT"
    And Enter input plugin property: "datetimeAfter" with value: "last.modified.after"
    Then Validate "Salesforce" plugin properties
    And Verify the Output Schema matches the Expected Schema: "account.schema"

  @BATCH-TS-SF-DSGN-04 @CONNECTION
  Scenario: Verify user should be able to create the valid connection using connection manager functionality
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And Click plugin property: "switch-useConnection"
    And Click on the Browse Connections button
    And Click on the Add Connection button
    And Click plugin property: "connector-Salesforce"
    And Enter input plugin property: "name" with value: "connection.name"
    And fill Authentication properties for Salesforce Admin user
    Then Click on the Test Connection button
    And Verify the test connection is successful
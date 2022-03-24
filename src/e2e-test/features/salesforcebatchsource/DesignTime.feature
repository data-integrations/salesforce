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
    Then verify the Output Schema table for an SOQL query of type: "<QueryType>"
    Examples:
      | QueryType     |
      | SIMPLE        |
      | WHERE         |
      | CHILDTOPARENT |

  @BATCH-TS-SF-DSGN-02
  Scenario Outline: Verify user should be able to get output schema for a valid SObject Name
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And configure Salesforce source for an SObject Query of SObject: "<SObjectName>"
    Then Validate "Salesforce" plugin properties
    And verify the Output Schema table for an SObject Query of SObject: "<SObjectName>"
    Examples:
      | SObjectName |
      | ACCOUNT     |
      | CONTACT     |
      | OPPORTUNITY |
      | LEAD        |

  @BATCH-TS-SF-DSGN-03
  Scenario: Verify user should be able to get output schema when plugin is configured with Last Modified After property
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And configure Salesforce source for an SObject Query of SObject: "OPPORTUNITY"
    And fill 'Last Modified After' property in format yyyy-MM-ddThh:mm:ssZ: "last.modified.after"
    Then Validate "Salesforce" plugin properties
    And verify the Output Schema table for an SObject Query of SObject: "OPPORTUNITY"

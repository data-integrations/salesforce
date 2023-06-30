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
@SFBatchSource
@Regression

Feature: Salesforce Batch Source - Design time - validation scenarios

  @BATCH-TS-SF-DSGN-ERROR-01
  Scenario: Verify required fields missing validation for 'Reference Name' property
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | referenceName |

  @BATCH-TS-SF-DSGN-ERROR-02
  Scenario: Verify validation message when user leaves Authentication Properties blank
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "empty.authentication.property" on the header

  @BATCH-TS-SF-DSGN-ERROR-03
  Scenario: Verify validation message when user provides invalid Authentication Properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And fill Authentication properties with invalid values
    And Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "invalid.authentication.property" on the header

  @BATCH-TS-SF-DSGN-ERROR-04
  Scenario: Verify required fields missing validation for SOQL Query or SObject Name properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "required.property.soqlorsobjectname.error" on the header
    And Verify that the Plugin Property: "query" is displaying an in-line error message: "required.property.soqlorsobjectname"

  @BATCH-TS-SF-DSGN-ERROR-05
  Scenario: Verify validation message for invalid SOQL Query property value
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And fill SOQL Query field with a Query: "STAR"
    And Click on the Get Schema button
    And Verify that the Plugin Property: "query" is displaying an in-line error message: "invalid.soql.starquery"

  @BATCH-TS-SF-DSGN-ERROR-06
  Scenario: Verify validation message for providing an invalid SObject Name in the SObject Query section
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And fill SObject Name property with an SObject Name: "Invalidobject"
    And Click on the Validate button
    Then Verify that the Plugin is displaying an error message: "invalid.sobjectname.error" on the header

  @BATCH-TS-SF-DSGN-ERROR-07
  Scenario: Verify validation message for invalid date formats used in Last Modified After and Before properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And configure Salesforce source for an SObject Query of SObject: "ACCOUNT"
    And Enter input plugin property: "datetimeAfter" with value: "abc"
    And Enter input plugin property: "datetimeBefore" with value: "abc"
    And Click on the Validate button
    Then Verify that the Plugin Property: "datetimeAfter" is displaying an in-line error message: "invalid.date.format.after"
    And Verify that the Plugin Property: "datetimeBefore" is displaying an in-line error message: "invalid.date.format.before"

  @BATCH-TS-SF-DSGN-ERROR-08
  Scenario: Verify user should be able to get invalid credentials validation message when using invalid credentials in the connection manager functionality
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And Click plugin property: "switch-useConnection"
    And Click on the Browse Connections button
    And Click on the Add Connection button
    And Click plugin property: "connector-Salesforce"
    And Enter input plugin property: "name" with value: "connection.name"
    And fill Authentication properties with invalid values
    Then Click on the Test Connection button
    Then Verify the invalid connection error message: "invalid.testconnection.logmessage" on the footer
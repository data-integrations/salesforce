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


Feature: Salesforce Multi Objects Batch Source - Design time - validation scenarios

  @MULTIBATCH-TS-SF-DSGN-ERROR-01
  Scenario: Verify validation message for incorrect SObject names in the White List
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SalesforceMultiObjects"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And fill White List with below listed SObjects:
      | ACCOUNTZZ | INVALID |
    And Click on the Validate button
    Then verify invalid SObject name validation message for White List

  @MULTIBATCH-TS-SF-DSGN-ERROR-02
  Scenario: Verify validation message for incorrect SObject names in the Black List
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SalesforceMultiObjects"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And fill Black List with below listed SObjects:
      | ACCOUNTZZ | LEADSS |
    And Click on the Validate button
    Then verify invalid SObject name validation message for Black List

  @MULTIBATCH-TS-SF-DSGN-ERROR-03
  Scenario: Verify validation message for invalid date formats used in Last Modified After and Before properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SalesforceMultiObjects"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And fill White List with below listed SObjects:
      | ACCOUNT | CONTACT |
    And Enter input plugin property: "datetimeAfter" with value: "abc"
    And Enter input plugin property: "datetimeBefore" with value: "abc"
    And Click on the Validate button
    Then Verify that the Plugin Property: "datetimeAfter" is displaying an in-line error message: "invalid.date.format.after"
    And Verify that the Plugin Property: "datetimeBefore" is displaying an in-line error message: "invalid.date.format.before"

  @MULTIBATCH-TS-SF-DSGN-ERROR-04
  Scenario: Verify user should be able to get invalid credentials validation message when using invalid credentials in the connection manager functionality
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SalesforceMultiObjects"
    And Click plugin property: "switch-useConnection"
    And Click on the Browse Connections button
    And Click on the Add Connection button
    And Click plugin property: "connector-Salesforce"
    And Enter input plugin property: "name" with value: "connection.name"
    And fill Authentication properties with invalid values
    Then Click on the Test Connection button
    Then Verify the invalid connection error message: "invalid.testconnection.logmessage" on the footer
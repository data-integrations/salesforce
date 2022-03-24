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
      | ACCOUNTZZ | BLAHBLAH |
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
    And fill 'Last Modified After' property in format yyyy-MM-ddThh:mm:ssZ: "abc"
    And fill 'Last Modified Before' property in format yyyy-MM-ddThh:mm:ssZ: "abc"
    And Click on the Validate button
    Then Verify that the Plugin Property: "datetimeAfter" is displaying an in-line error message: "invalid.date.format"
    And Verify that the Plugin Property: "datetimeBefore" is displaying an in-line error message: "invalid.date.format"

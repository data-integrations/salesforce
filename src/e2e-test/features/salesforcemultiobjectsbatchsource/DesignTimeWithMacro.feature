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
@SourceAndSink
Feature: Salesforce Multi Objects Batch Source - Design time Scenarios (macros)

  @MULTIBATCH-TS-SF-DSGN-MACRO-01
  Scenario: Verify user should be able to validate the plugin when Authentication section is configured for macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SalesforceMultiObjects"
    And fill Reference Name property
    And Click on the Macro button of Property: "username" and set the value to: "Username"
    And Click on the Macro button of Property: "password" and set the value to: "Password"
    And Click on the Macro button of Property: "securityToken" and set the value to: "SecurityToken"
    And Click on the Macro button of Property: "consumerKey" and set the value to: "ConsumerKey"
    And Click on the Macro button of Property: "consumerSecret" and set the value to: "ConsumerSecret"
    And Click on the Macro button of Property: "loginUrl" and set the value to: "LoginUrl"
    And Click on the Macro button of Property: "whiteList" and set the value to: "WhiteList"
    Then Validate "SalesforceMultiObjects" plugin properties

  @MULTIBATCH-TS-SF-DSGN-MACRO-02
  Scenario: Verify user should be able to validate the plugin when configured for White List with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SalesforceMultiObjects"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And Click on the Macro button of Property: "whiteList" and set the value to: "WhiteList"
    Then Validate "SalesforceMultiObjects" plugin properties

  @MULTIBATCH-TS-SF-DSGN-MACRO-03
  Scenario: Verify user should be able to validate the plugin when configured for Black List with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SalesforceMultiObjects"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And Click on the Macro button of Property: "blackList" and set the value to: "BlackList"
    Then Validate "SalesforceMultiObjects" plugin properties

  @MULTIBATCH-TS-SF-DSGN-MACRO-04
  Scenario: Verify user should be able to validate the plugin when Incremental Load Properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce Multi Objects" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SalesforceMultiObjects"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And Click on the Macro button of Property: "whiteList" and set the value to: "WhiteList"
    And Click on the Macro button of Property: "datetimeAfter" and set the value to: "datetimeAfter"
    And Click on the Macro button of Property: "datetimeBefore" and set the value to: "datetimeBefore"
    And Click on the Macro button of Property: "duration" and set the value to: "duration"
    And Click on the Macro button of Property: "offset" and set the value to: "offset"
    Then Validate "SalesforceMultiObjects" plugin properties

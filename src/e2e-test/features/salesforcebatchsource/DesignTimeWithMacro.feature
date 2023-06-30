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

Feature: Salesforce Batch Source - Design time Scenarios (macro)

  @BATCH-TS-SF-DSGN-MACRO-01
  Scenario:Verify user should be able to validate the plugin when Authentication properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And Click on the Macro button of Property: "username" and set the value to: "username"
    And Click on the Macro button of Property: "password" and set the value to: "password"
    And Click on the Macro button of Property: "securityToken" and set the value to: "securityToken"
    And Click on the Macro button of Property: "consumerKey" and set the value to: "consumerKey"
    And Click on the Macro button of Property: "consumerSecret" and set the value to: "consumerSecret"
    And Click on the Macro button of Property: "loginUrl" and set the value to: "loginUrl"
    And Click on the Macro button of Property: "sObjectName" and set the value to: "sObjectName"
    Then Validate "Salesforce" plugin properties

  @BATCH-TS-SF-DSGN-MACRO-02
  Scenario: Verify user should be able to validate the plugin when SObject Query properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And Click on the Macro button of Property: "sObjectName" and set the value to: "sObjectName"
    And Click on the Macro button of Property: "datetimeAfter" and set the value to: "lastModifiedAfter"
    And Click on the Macro button of Property: "datetimeBefore" and set the value to: "lastModifiedBefore"
    And Click on the Macro button of Property: "duration" and set the value to: "duration"
    And Click on the Macro button of Property: "offset" and set the value to: "offset"
    Then Validate "Salesforce" plugin properties
    
  @BATCH-TS-SF-DSGN-MACRO-03
  Scenario: Verify user should be able to validate the plugin when Advanced section properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And Click on the Macro button of Property: "sObjectName" and set the value to: "sObjectName"
    And Click on the Macro button of Property: "enablePKChunk" and set the value to: "enablePKChunk"
    And Click on the Macro button of Property: "chunkSize" and set the value to: "chunkSize"
    Then Validate "Salesforce" plugin properties

  @BATCH-TS-SF-DSGN-MACRO-04
  Scenario: Verify user should be able to validate the plugin when SOQL Query properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "Salesforce" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And Click on the Macro button of Property: "query" and set the value in textarea: "query"
    Then Validate "Salesforce" plugin properties

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
@Regression
Feature: Salesforce Batch Source - Design time - validation scenarios

  @BATCH-TS-SF-DSGN-02
  Scenario: Verify required fields missing validation for 'Reference Name' property
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as 'Data Pipeline - Batch'
    And Select plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And click on the Validate button
    Then verify required fields missing validation message for Reference Name property

  @BATCH-TS-SF-DSGN-04
  Scenario: Verify validation message when user leaves Authentication Properties fields blank
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as 'Data Pipeline - Batch'
    And Select plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And click on the Validate button
    Then verify validation message for blank Authentication properties

  @BATCH-TS-SF-DSGN-27
  Scenario: Verify validation message when user provides invalid Authentication Properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as 'Data Pipeline - Batch'
    And Select plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And fill Authentication properties with invalid values
    And click on the Validate button
    Then verify validation message for invalid Authentication properties

  @BATCH-TS-SF-DSGN-05
  Scenario: Verify required fields missing validation for SOQL Query or SObject Name properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as 'Data Pipeline - Batch'
    And Select plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And click on the Validate button
    And verify validation message for missing SOQL or SObject Name property

  @BATCH-TS-SF-DSGN-06
  Scenario: Verify validation message for invalid SOQL Query property value
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as 'Data Pipeline - Batch'
    And Select plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And fill SOQL Query field with a Star Query
    And click on the Get Schema button
    And verify validation message for invalid soql query with Star

  @BATCH-TS-SF-DSGN-08
  Scenario: Verify validation message for providing an invalid SObject Name in the SObject Query section
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as 'Data Pipeline - Batch'
    And Select plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And fill SObject Name property with an invalid value
    And click on the Validate button
    And verify validation message for invalid SObject name

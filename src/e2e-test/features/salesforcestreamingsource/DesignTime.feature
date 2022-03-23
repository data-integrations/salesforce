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
@@SFStreamingSource
@Smoke
@Regression
Feature: Salesforce Streaming Source - Design time scenarios

  @STREAMING-TS-SF-DSGN-06
  Scenario: Verify user should be able to successfully validate the source for valid Topic name
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Realtime"
    And Select Realtime Source plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And fill Topic Name as: "TopicName"
    And select option for notifyOnCreate as ENABLED
    And select option for notifyOnUpdate as DISABLED
    And select option for notifyOn Delete as ENABLED
    Then select option for notifyForFields as SELECT
    Then Validate "Salesforce" plugin properties



  @STREAMING-TS-SF-DSGN-07
  Scenario Outline: Verify user should be able to get output schema for a valid SObject Name
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Realtime"
    And Select Realtime Source plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And fill Topic Name as: "TopicName2"
    And configure Salesforce source for an SObject Query of SObject: "<SObjectName>"
    And select option for notifyOnCreate as ENABLED
    And select option for notifyOnUpdate as DISABLED
    And select option for notifyOn Delete as ENABLED
    And select option for notifyForFields as SELECT
    Then Validate "Salesforce" plugin properties
    And verify the Output Schema table for an SObject Query of SObject: "<SObjectName>"
    Examples:
      | SObjectName |
      | ACCOUNT     |
      | OPPORTUNITY |

  @STREAMING-TS-SF-DSGN-08
  Scenario Outline: Verify user should be able to get output schema for a valid Topic Query
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Realtime"
    And Select Realtime Source plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And fill Topic Name as: "TopicName3"
    And configure Salesforce source for an Push Topic Query of type: "<QueryType>"
    And select option for notifyOnCreate as ENABLED
    And select option for notifyOnUpdate as DISABLED
    And select option for notifyOn Delete as ENABLED
    And select option for notifyForFields as SELECT
    Then Validate "Salesforce" plugin properties
    Examples:
      | QueryType |
      | SIMPLE    |




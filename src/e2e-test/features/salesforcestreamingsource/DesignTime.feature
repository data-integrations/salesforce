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

  @STREAMING-TS-SF-DSGN-01
  Scenario: Verify user should be able to successfully validate the source for an existing Topic name
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Realtime"
    And Select Realtime Source plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And Enter input plugin property: "pushTopicName" with value: "topic.name"
    And Select dropdown plugin property: "pushTopicNotifyCreate" with option value: "Enabled"
    And Select dropdown plugin property: "pushTopicNotifyUpdate" with option value: "Enabled"
    And Select dropdown plugin property: "pushTopicNotifyDelete" with option value: "Enabled"
    And Select dropdown plugin property: "pushTopicNotifyForFields" with option value: "Referenced"
    And Enter input plugin property: "referenceName" with value: "RefName"
    Then Validate "Salesforce" plugin properties

  @STREAMING-TS-SF-DSGN-02 @DELETE_PUSH_TOPIC
  Scenario Outline: Verify user should be able to get output schema for a new Topic name created for an SObject Name
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Realtime"
    And Select Realtime Source plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And fill Topic Name field with a unique value
    And configure Salesforce source for an SObject Query of SObject: "<SObjectName>"
    And Select dropdown plugin property: "pushTopicNotifyCreate" with option value: "Enabled"
    And Select dropdown plugin property: "pushTopicNotifyUpdate" with option value: "Disabled"
    And Select dropdown plugin property: "pushTopicNotifyDelete" with option value: "Enabled"
    And Select dropdown plugin property: "pushTopicNotifyForFields" with option value: "Select"
    Then Validate "Salesforce" plugin properties
    Examples:
      | SObjectName |
      | LEAD        |

  @STREAMING-TS-SF-DSGN-03 @DELETE_PUSH_TOPIC
  Scenario Outline: Verify user should be able to get output schema for a new Topic name created for a SOQL Query
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Realtime"
    And Select Realtime Source plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Authentication properties for Salesforce Admin user
    And fill Topic Name field with a unique value
    And configure Salesforce source for an Push Topic Query of type: "<QueryType>"
    And Select dropdown plugin property: "pushTopicNotifyCreate" with option value: "Enabled"
    And Select dropdown plugin property: "pushTopicNotifyUpdate" with option value: "Disabled"
    And Select dropdown plugin property: "pushTopicNotifyDelete" with option value: "Enabled"
    And Select dropdown plugin property: "pushTopicNotifyForFields" with option value: "Select"
    Then Validate "Salesforce" plugin properties
    Examples:
      | QueryType |
      | SIMPLE    |

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
Feature: Salesforce Streaming Source - Design time - validation scenarios

  @STREAMING-TS-SF-DSGN-ERROR-01
  Scenario: Verify required fields missing validation for 'Topic name' property
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Realtime"
    And Select Realtime Source plugin: "Salesforce" from the plugins list
    And Navigate to the properties page of plugin: "Salesforce"
    And fill Reference Name property
    And fill Authentication properties for Salesforce Admin user
    And Click on the Validate button
    Then Verify that the Plugin Property: "pushTopicName" is displaying an in-line error message: "required.property.topicname"

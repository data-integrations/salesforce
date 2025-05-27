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

@ServiceNow
@SNMultiSource
@Smoke
@Regression
Feature: ServiceNow Multi Source - Run time scenarios (macro)

  @TS-SN-MULTI-RNTM-MACRO-01 @BQ_SINK
  Scenario: Verify user should be able to preview the pipeline when the source plugin is configured with macros
    When Open Datafusion Project to configure pipeline
    And Select plugin: "ServiceNow Multi Source" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "ServiceNow Multi Source"
    And Fill Reference Name
    And Click on the Macro button of Property: "tableNames" and set the value to: "tableNames"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "restApiEndpoint" and set the value to: "restApiEndpoint"
    And Click on the Macro button of Property: "user" and set the value to: "username"
    And Click on the Macro button of Property: "password" and set the value to: "password"
    Then Validate "ServiceNow Multi Source" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryMultiTable" from the plugins list
    And Connect source as "ServiceNow" and sink as "BigQueryMultiTable" to establish connection
    And Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Configure BigQuery Multi Table sink plugin for Dataset
    Then Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    And Preview and run the pipeline
    And Enter runtime argument value "receiving_slip_line" for key "tableNames"
    And Enter runtime argument value from environment variable "client.id" for key "clientId"
    And Enter runtime argument value from environment variable "client.secret" for key "clientSecret"
    And Enter runtime argument value from environment variable "rest.api.endpoint" for key "restApiEndpoint"
    And Enter runtime argument value from environment variable "pipeline.user.username" for key "username"
    And Enter runtime argument value from environment variable "pipeline.user.password" for key "password"
    Then Run the preview of pipeline with runtime arguments
    Then Verify the preview of pipeline is "success"

  @TS-SN-MULTI-RNTM-MACRO-02 @BQ_SINK
  Scenario: Verify user should be able to run the pipeline when the source plugin is configured with macros
    When Open Datafusion Project to configure pipeline
    And Select plugin: "ServiceNow Multi Source" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "ServiceNow Multi Source"
    And Fill Reference Name
    And Click on the Macro button of Property: "tableNames" and set the value to: "tableNames"
    And Click on the Macro button of Property: "clientId" and set the value to: "clientId"
    And Click on the Macro button of Property: "clientSecret" and set the value to: "clientSecret"
    And Click on the Macro button of Property: "restApiEndpoint" and set the value to: "restApiEndpoint"
    And Click on the Macro button of Property: "user" and set the value to: "username"
    And Click on the Macro button of Property: "password" and set the value to: "password"
    Then Validate "ServiceNow Multi Source" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryMultiTable" from the plugins list
    And Connect source as "ServiceNow" and sink as "BigQueryMultiTable" to establish connection
    And Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Configure BigQuery Multi Table sink plugin for Dataset
    Then Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "receiving_slip_line" for key "tableNames"
    And Enter runtime argument value from environment variable "client.id" for key "clientId"
    And Enter runtime argument value from environment variable "client.secret" for key "clientSecret"
    And Enter runtime argument value from environment variable "rest.api.endpoint" for key "restApiEndpoint"
    And Enter runtime argument value from environment variable "pipeline.user.username" for key "username"
    And Enter runtime argument value from environment variable "pipeline.user.password" for key "password"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running status with a timeout of 500 seconds
    And Verify the pipeline status is "Succeeded"

  @TS-SN-MULTI-RNTM-MACRO-03 @BQ_SINK
  Scenario: Verify pipeline failure message in logs when user provides invalid Table Names with Macros
    When Open Datafusion Project to configure pipeline
    And Select plugin: "ServiceNow Multi Source" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "ServiceNow Multi Source"
    And Fill Reference Name
    And Click on the Macro button of Property: "tableNames" and set the value to: "tableNames"
    And fill Credentials section for pipeline user
    Then Validate "ServiceNow Multi Source" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryMultiTable" from the plugins list
    And Connect source as "ServiceNow-Multi-Source" and sink as "BigQueryMultiTable" to establish connection
    And Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Configure BigQuery Multi Table sink plugin for Dataset
    Then Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "invalid.tables" for key "tableNames"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                   |
      | ERROR | invalid.tablename.logsmessage            |

  @TS-SN-RNTM-MACRO-04 @BQ_SINK
  Scenario: Verify pipeline failure message in logs when user provides invalid Advanced Properties with Macros
    When Open Datafusion Project to configure pipeline
    And Select plugin: "ServiceNow Multi Source" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "ServiceNow Multi Source"
    And Fill Reference Name
    And configure ServiceNow Multi source plugin for below listed tables:
      | HARDWARE_CATALOG |
    And Click on the Macro button of Property: "startDate" and set the value to: "startDate"
    And Click on the Macro button of Property: "endDate" and set the value to: "endDate"
    And fill Credentials section for pipeline user
    Then Validate "ServiceNow Multi Source" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryMultiTable" from the plugins list
    And Connect source as "ServiceNow-Multi-Source" and sink as "BigQueryMultiTable" to establish connection
    And Navigate to the properties page of plugin: "BigQuery Multi Table"
    And Configure BigQuery Multi Table sink plugin for Dataset
    Then Validate "BigQuery Multi Table" plugin properties
    And Close the Plugin Properties page
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "invalid.start.date" for key "startDate"
    And Enter runtime argument value "invalid.end.date" for key "endDate"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                |
      | ERROR | invalid.filters.logsmessage            |

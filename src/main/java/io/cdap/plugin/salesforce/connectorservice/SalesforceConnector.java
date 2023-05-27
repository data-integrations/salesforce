/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.salesforce.connectorservice;

import com.google.cloud.connector.api.AssetName;
import com.google.cloud.connector.api.Connector;
import com.google.cloud.connector.api.ConnectorContext;
import com.google.cloud.connector.api.annotation.DataSource;
import com.google.cloud.connector.api.schema.SchemaBuilder;

import static com.google.cloud.bigquery.federation.v1alpha1.DataSource.Capability.SUPPORTS_SYNCHRONOUS_QUERIES;

/** Connector to test connector loading. */
public class SalesforceConnector implements Connector {

  private final SalesforceConnectorConfig config;

  @DataSource(capabilities = {SUPPORTS_SYNCHRONOUS_QUERIES})
  public SalesforceConnector(SalesforceConnectorConfig config) {
    this.config = config;
  }

  @Override
  public String toString() {
    return config.toString();
  }

  @Override
  public void ResolveSchema(AssetName assetName, ConnectorContext context) {
    // TODO(wyzhang):
    SchemaBuilder builder = context.getSchemaBuilder();
  }
}

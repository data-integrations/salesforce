/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.salesforce.plugin.source.batch;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.salesforce.SalesforceConstants;
import io.cdap.salesforce.plugin.source.batch.util.SalesforceSourceConstants;

import java.util.Map;

/**
 * InputFormatProvider used by cdap to provide configurations to mapreduce job
 */
public class SalesforceInputFormatProvider implements InputFormatProvider {
  private final Map<String, String> conf;

  SalesforceInputFormatProvider(SalesforceSourceConfig config) {
    String query = config.getQuery();
    this.conf = new ImmutableMap.Builder<String, String>()
      .put(SalesforceConstants.CONFIG_USERNAME, config.getUsername())
      .put(SalesforceConstants.CONFIG_PASSWORD, config.getPassword())
      .put(SalesforceConstants.CONFIG_CLIENT_ID, config.getClientId())
      .put(SalesforceConstants.CONFIG_CLIENT_SECRET, config.getClientSecret())
      .put(SalesforceConstants.CONFIG_LOGIN_URL, config.getLoginUrl())
      .put(SalesforceConstants.CONFIG_ERROR_HANDLING, config.getErrorHandling())
      .put(SalesforceSourceConstants.CONFIG_QUERY, query)
      .build();
  }

  @Override
  public String getInputFormatClassName() {
    return SalesforceInputFormat.class.getName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return conf;
  }
}

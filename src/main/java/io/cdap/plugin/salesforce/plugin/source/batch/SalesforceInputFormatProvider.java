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

package io.cdap.plugin.salesforce.plugin.source.batch;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * InputFormatProvider used by cdap to provide configurations to mapreduce job
 */
public class SalesforceInputFormatProvider implements InputFormatProvider {

  private static final Gson GSON = new Gson();

  private final Map<String, String> conf;

  public SalesforceInputFormatProvider(SalesforceBaseSourceConfig config,
                                       Map<String, String> schemas,
                                       List<SalesforceSplit> querySplits,
                                       @Nullable String sObjectNameField) {
    ImmutableMap.Builder<String, String> configBuilder = new ImmutableMap.Builder<String, String>()
      .put(SalesforceSourceConstants.CONFIG_SCHEMAS, GSON.toJson(schemas));
    configBuilder.put(SalesforceSourceConstants.CONFIG_QUERY_SPLITS, GSON.toJson(querySplits));
      OAuthInfo oAuthInfo = config.getConnection().getOAuthInfo();
      if (oAuthInfo != null) {
        configBuilder
          .put(SalesforceConstants.CONFIG_OAUTH_TOKEN, oAuthInfo.getAccessToken())
          .put(SalesforceConstants.CONFIG_OAUTH_INSTANCE_URL, oAuthInfo.getInstanceURL());
      } else {
        configBuilder
          .put(SalesforceConstants.CONFIG_USERNAME, Objects.requireNonNull(config.getConnection().getUsername()))
          .put(SalesforceConstants.CONFIG_PASSWORD, Objects.requireNonNull(config.getConnection().getPassword()))
          .put(SalesforceConstants.CONFIG_CONSUMER_KEY, Objects.requireNonNull(config.getConnection().getConsumerKey()))
          .put(SalesforceConstants.CONFIG_CONSUMER_SECRET, Objects.requireNonNull(config.getConnection().
                                                                                    getConsumerSecret()))
          .put(SalesforceConstants.CONFIG_LOGIN_URL, Objects.requireNonNull(config.getConnection().getLoginUrl()));
      }
    if (sObjectNameField != null) {
      configBuilder.put(SalesforceSourceConstants.CONFIG_SOBJECT_NAME_FIELD, sObjectNameField);
    }
    this.conf = configBuilder.build();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return conf;
  }

  @Override
  public String getInputFormatClassName() {
    return SalesforceInputFormat.class.getName();
  }
}

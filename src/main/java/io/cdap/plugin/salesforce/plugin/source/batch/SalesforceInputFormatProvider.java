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
                                       List<String> queries,
                                       Map<String, String> schemas,
                                       @Nullable String sObjectNameField) {
    ImmutableMap.Builder<String, String> configBuilder = new ImmutableMap.Builder<String, String>()
      .put(SalesforceSourceConstants.CONFIG_QUERIES, GSON.toJson(queries))
      .put(SalesforceSourceConstants.CONFIG_SCHEMAS, GSON.toJson(schemas));

    OAuthInfo oAuthInfo = config.getOAuthInfo();
    if (oAuthInfo != null) {
      configBuilder
        .put(SalesforceConstants.CONFIG_OAUTH_TOKEN, oAuthInfo.getAccessToken())
        .put(SalesforceConstants.CONFIG_OAUTH_INSTANCE_URL, oAuthInfo.getInstanceURL());
    } else {
      configBuilder
        .put(SalesforceConstants.CONFIG_USERNAME, Objects.requireNonNull(config.getUsername()))
        .put(SalesforceConstants.CONFIG_PASSWORD, Objects.requireNonNull(config.getPassword()))
        .put(SalesforceConstants.CONFIG_CONSUMER_KEY, Objects.requireNonNull(config.getConsumerKey()))
        .put(SalesforceConstants.CONFIG_CONSUMER_SECRET, Objects.requireNonNull(config.getConsumerSecret()))
        .put(SalesforceConstants.CONFIG_LOGIN_URL, Objects.requireNonNull(config.getLoginUrl()));
    }

    if (sObjectNameField != null) {
      configBuilder.put(SalesforceSourceConstants.CONFIG_SOBJECT_NAME_FIELD, sObjectNameField);
    }

    this.conf = configBuilder.build();
  }

  public SalesforceInputFormatProvider(SalesforceSourceConfig config,
                                       List<String> queries,
                                       Map<String, String> schemas,
                                       @Nullable String sObjectNameField) {
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>()
      .put(SalesforceConstants.CONFIG_USERNAME, config.getUsername())
      .put(SalesforceConstants.CONFIG_PASSWORD, config.getPassword())
      .put(SalesforceConstants.CONFIG_CONSUMER_KEY, config.getConsumerKey())
      .put(SalesforceConstants.CONFIG_CONSUMER_SECRET, config.getConsumerSecret())
      .put(SalesforceConstants.CONFIG_LOGIN_URL, config.getLoginUrl())
      .put(SalesforceSourceConstants.CONFIG_QUERIES, GSON.toJson(queries))
      .put(SalesforceSourceConstants.CONFIG_SCHEMAS, GSON.toJson(schemas))
      .put(SalesforceSourceConstants.CONFIG_PK_CHUNK_ENABLE, String.valueOf(config.getEnablePKChunk()))
      .put(SalesforceSourceConstants.CONFIG_CHUNK_SIZE, String.valueOf(config.getChunkSize()));

    if (sObjectNameField != null) {
      builder.put(SalesforceSourceConstants.CONFIG_SOBJECT_NAME_FIELD, sObjectNameField);
    }

    this.conf = builder.build();
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

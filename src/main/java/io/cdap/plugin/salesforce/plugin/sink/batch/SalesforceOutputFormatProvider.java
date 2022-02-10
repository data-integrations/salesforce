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
package io.cdap.plugin.salesforce.plugin.sink.batch;

import com.google.common.collect.ImmutableMap;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import com.sforce.async.JobInfo;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.plugin.salesforce.SalesforceBulkUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/**
 *  Provides SalesforceOutputFormat's class name and configuration.
 */
public class SalesforceOutputFormatProvider implements OutputFormatProvider {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceOutputFormatProvider.class);

  private final Map<String, String> configMap;

  /**
   * Gets properties from config and stores them as properties in map for Mapreduce.
   *
   * @param config Salesforce batch sink configuration
   */
  public SalesforceOutputFormatProvider(SalesforceSinkConfig config) {
    ImmutableMap.Builder<String, String> configBuilder = new ImmutableMap.Builder<String, String>()
      .put(SalesforceSinkConstants.CONFIG_SOBJECT, config.getSObject())
      .put(SalesforceSinkConstants.CONFIG_OPERATION, config.getOperation())
      .put(SalesforceSinkConstants.CONFIG_ERROR_HANDLING, config.getErrorHandling().getValue())
      .put(SalesforceSinkConstants.CONFIG_MAX_BYTES_PER_BATCH, config.getMaxBytesPerBatch().toString())
      .put(SalesforceSinkConstants.CONFIG_MAX_RECORDS_PER_BATCH, config.getMaxRecordsPerBatch().toString());

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

    if (config.getExternalIdField() != null) {
      configBuilder.put(SalesforceSinkConstants.CONFIG_EXTERNAL_ID_FIELD, config.getExternalIdField());
    }

    AuthenticatorCredentials credentials = config.getAuthenticatorCredentials();

    try {
      BulkConnection bulkConnection = new BulkConnection(Authenticator.createConnectorConfig(credentials));
      JobInfo job = SalesforceBulkUtil.createJob(bulkConnection, config.getSObject(), config.getOperationEnum(),
                                                 config.getExternalIdField());
      configBuilder.put(SalesforceSinkConstants.CONFIG_JOB_ID, job.getId());
      LOG.info("Started Salesforce job with jobId='{}'", job.getId());
    } catch (AsyncApiException e) {
      throw new RuntimeException("There was issue communicating with Salesforce", e);
    }

    this.configMap = configBuilder.build();
  }

  @Override
  public String getOutputFormatClassName() {
    return SalesforceOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return configMap;
  }
}

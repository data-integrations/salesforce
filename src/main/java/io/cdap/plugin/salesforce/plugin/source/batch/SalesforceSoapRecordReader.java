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

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * RecordReader implementation for SOQL queries with restricted field types (function calls, sub-query fields) or
 * GROUP BY [ROLLUP / CUBE], OFFSET clauses. Reads Salesforce query and makes SOAP calls to retrieve all values.
 */
public class SalesforceSoapRecordReader extends RecordReader<Schema, Map<String, ?>> {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceSoapRecordReader.class);

  private final Schema schema;
  private final String query;
  private final SoapRecordToMapTransformer transformer;
  private SObjectDescriptor sObjectDescriptor;
  private PartnerConnection partnerConnection;
  private QueryResult queryResult;
  private SObject[] sObjects;
  private int index;

  private Map<String, ?> value;

  public SalesforceSoapRecordReader(Schema schema, String query, SoapRecordToMapTransformer transformer) {
    this.schema = schema;
    this.query = query;
    this.transformer = transformer;
  }

  /**
   * Get XML objects from Salesforce query
   *
   * @param inputSplit         specifies batch details
   * @param taskAttemptContext task context
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    LOG.debug("Executing Salesforce SOAP query: '{}'", query);

    Configuration conf = taskAttemptContext.getConfiguration();
    try {
      AuthenticatorCredentials credentials = SalesforceConnectionUtil.getAuthenticatorCredentials(conf);
      partnerConnection = SalesforceConnectionUtil.getPartnerConnection(credentials);
      sObjectDescriptor = SObjectDescriptor.fromQuery(query);
      queryResult = partnerConnection.query(query);
    } catch (ConnectionException e) {
      String errorMessage = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      throw new RuntimeException(
        String.format("Failed to create a Salesforce SOAP connection to execute query %s: %s",
                      query,
                      errorMessage),
        e);
    }
  }

  /**
   * Reads single record from query results.
   * Fetches more records if available.
   *
   * @return returns false if no more data to read
   */
  @Override
  public boolean nextKeyValue() throws IOException {
    if (readValue()) {
      return true;
    }

    if (queryResult.isDone()) {
      return false;
    }

    queryMore();
    return readValue();

  }

  @Override
  public Schema getCurrentKey() {
    return schema;
  }

  @Override
  public Map<String, ?> getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() {
    return 0.0f;
  }

  @Override
  public void close() {
    // no-op
  }

  private boolean readValue() {
    if (sObjects == null) {
      index = 0;
      sObjects = queryResult.getRecords();
    }
    if (sObjects.length > index) {
      value = transformer.transformToMap(sObjects[index++], sObjectDescriptor);
      return true;
    }
    return false;
  }

  private void queryMore() throws IOException {
    try {
      sObjects = null;
      queryResult = partnerConnection.queryMore(queryResult.getQueryLocator());
    } catch (ConnectionException e) {
      String errorMessage = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      throw new IOException(String.format("Cannot create Salesforce SOAP connection for query locator: '%s' :%s",
                                          queryResult.getQueryLocator(), errorMessage), e);
    }
  }
}

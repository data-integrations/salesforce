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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BulkConnection;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceBulkUtil;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceQueryUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.parser.SalesforceQueryParser;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Input format class which generates input splits for each given query
 * and initializes appropriate record reader.
 */
public class SalesforceInputFormat extends InputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceInputFormat.class);

  private static final Gson GSON = new Gson();
  private static final Type QUERIES_TYPE = new TypeToken<List<String>>() { }.getType();
  private static final Type SCHEMAS_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    Configuration configuration = context.getConfiguration();
    List<String> queries = GSON.fromJson(configuration.get(SalesforceSourceConstants.CONFIG_QUERIES), QUERIES_TYPE);
    BulkConnection bulkConnection = getBulkConnection(configuration);

    boolean enablePKChunk = configuration.getBoolean(SalesforceSourceConstants.CONFIG_PK_CHUNK_ENABLE, false);
    if (enablePKChunk) {
      int chunkSize = configuration.getInt(SalesforceSourceConstants.CONFIG_CHUNK_SIZE,
                                           SalesforceSourceConstants.DEFAULT_PK_CHUNK_SIZE);
      bulkConnection.addHeader(SalesforceSourceConstants.HEADER_ENABLE_PK_CHUNK,
                               String.format(SalesforceSourceConstants.HEADER_VALUE_PK_CHUNK, chunkSize));
    }

    return queries.parallelStream()
      .map(query -> getQuerySplits(query, bulkConnection, enablePKChunk))
      .flatMap(Collection::stream)
      .collect(Collectors.toList());
  }

  @Override
  public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
    SalesforceSplit multiSplit = (SalesforceSplit) split;
    String query = multiSplit.getQuery();

    SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(query);
    String sObjectName = sObjectDescriptor.getName();

    Configuration configuration = context.getConfiguration();
    String sObjectNameField = configuration.get(SalesforceSourceConstants.CONFIG_SOBJECT_NAME_FIELD);
    Map<String, String> schemas = GSON.fromJson(
      configuration.get(SalesforceSourceConstants.CONFIG_SCHEMAS), SCHEMAS_TYPE);
    Schema schema = Schema.parseJson(schemas.get(sObjectName));

    return new SalesforceRecordReaderWrapper(sObjectName, sObjectNameField, getDelegateRecordReader(query, schema));
  }

  private List<SalesforceSplit> getQuerySplits(String query, BulkConnection bulkConnection, boolean enablePKChunk) {
    return Stream.of(getBatches(query, bulkConnection, enablePKChunk))
      .map(batch -> new SalesforceSplit(batch.getJobId(), batch.getId(), query))
      .collect(Collectors.toList());
  }

  /**
   * Initializes bulk connection based on given Hadoop configuration.
   *
   * @param conf Hadoop configuration
   * @return bulk connection instance
   */
  private BulkConnection getBulkConnection(Configuration conf) {
    try {
      AuthenticatorCredentials credentials = SalesforceConnectionUtil.getAuthenticatorCredentials(conf);
      return new BulkConnection(Authenticator.createConnectorConfig(credentials));
    } catch (AsyncApiException e) {
      throw new RuntimeException("There was issue communicating with Salesforce", e);
    }
  }

  /**
   * Based on query length sends query to Salesforce to receive array of batch info.
   * If query is within limit, executes original query. If not, switches to wide object logic,
   * i.e. generates Id query to retrieve batch info for Ids only that will be used later
   * to retrieve data using SOAP API.
   *
   * @param query SOQL query
   * @param bulkConnection bulk connection
   * @param enablePKChunk enable PK Chunking
   * @return array of batch info
   */
  private BatchInfo[] getBatches(String query, BulkConnection bulkConnection, boolean enablePKChunk) {
    try {
      if (!SalesforceQueryUtil.isQueryUnderLengthLimit(query)) {
        LOG.debug("Wide object query detected. Query length '{}'", query.length());
        query = SalesforceQueryUtil.createSObjectIdQuery(query);
      }
       BatchInfo[] batches = SalesforceBulkUtil.runBulkQuery(bulkConnection, query, enablePKChunk);
      LOG.debug("Number of batches received from Salesforce: '{}'", batches.length);
      return batches;
    } catch (AsyncApiException | IOException e) {
      throw new RuntimeException("There was issue communicating with Salesforce", e);
    }
  }

  private RecordReader<Schema, Map<String, ?>> getDelegateRecordReader(String query, Schema schema) {
    if (SalesforceQueryParser.isRestrictedQuery(query)) {
      LOG.info("The SOQL query uses an aggregate function call or offset. "
        + "Reads will be performed serially and not in parallel.");
      return new SalesforceSoapRecordReader(schema, query, new SoapRecordToMapTransformer());
    }
    if (SalesforceQueryUtil.isQueryUnderLengthLimit(query)) {
      return new SalesforceBulkRecordReader(schema);
    }
    LOG.info("The SOQL query is a wide query. "
      + "An additional SOAP request will be performed for each record.");
    return new SalesforceWideRecordReader(schema, query, new SoapRecordToMapTransformer());
  }
}

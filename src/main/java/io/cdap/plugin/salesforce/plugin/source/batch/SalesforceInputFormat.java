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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceQueryUtil;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Input format class which generates input splits for each given query and initializes appropriate record reader.
 */
public class SalesforceInputFormat extends InputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceInputFormat.class);

  private static final Gson GSON = new Gson();
  private static final Type SCHEMAS_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type QUERY_SPLITS_TYPE = new TypeToken<List<SalesforceSplit>>() { }.getType();

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    Configuration configuration = context.getConfiguration();
    List<SalesforceSplit> querySplits = GSON.fromJson(
      configuration.get(SalesforceSourceConstants.CONFIG_QUERY_SPLITS), QUERY_SPLITS_TYPE);
    // this is needed to convert List<SalesforceSplits> to List<InputSplit>
    return querySplits.parallelStream().collect(Collectors.toList());
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

  public static RecordReader createInitedRecordReader(
      SalesforceSplit multiSplit, String schemaJson, AuthenticatorCredentials credentials)
      throws IOException, InterruptedException {
    String query = multiSplit.getQuery();
    SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(query);
    String sObjectName = sObjectDescriptor.getName();
    Schema schema = Schema.parseJson(schemaJson);
    SalesforceRecordReaderWrapper readerWrapper = new SalesforceRecordReaderWrapper(
        sObjectName, null, getInitedDelegateRecordReader(query, schema, multiSplit, credentials));
    return readerWrapper;
  }

  private static RecordReader<Schema, Map<String, ?>> getDelegateRecordReader(String query,
      Schema schema) {
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

  private static RecordReader<Schema, Map<String, ?>> getInitedDelegateRecordReader(
      String query, Schema schema, SalesforceSplit split, AuthenticatorCredentials credentials)
      throws IOException, InterruptedException {
    if (SalesforceQueryParser.isRestrictedQuery(query)) {
      LOG.info("The SOQL query uses an aggregate function call or offset. "
                 + "Reads will be performed serially and not in parallel.");

      return (new SalesforceSoapRecordReader(schema, query, new SoapRecordToMapTransformer()))
          .initialize(credentials);
    }
    if (SalesforceQueryUtil.isQueryUnderLengthLimit(query)) {
      return (new SalesforceBulkRecordReader(schema))
          .initialize(split, credentials);
    }
    LOG.info("The SOQL query is a wide query. "
               + "An additional SOAP request will be performed for each record.");
    return new SalesforceWideRecordReader(schema, query, new SoapRecordToMapTransformer());
  }
}

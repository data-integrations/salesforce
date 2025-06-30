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

import com.google.common.collect.Lists;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * RecordReader implementation for wide SOQL queries. Reads a single Salesforce batch of SObject Id's from bulk job
 * provided in InputSplit, creates subpartitions and makes parallel SOAP calls to retrieve all values.
 */
public class SalesforceWideRecordReader extends SalesforceBulkRecordReader {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceWideRecordReader.class);

  private final String query;
  private final SoapRecordToMapTransformer transformer;
  private Iterator<List<Map<String, ?>>> batchIterator;

  private List<Map<String, ?>> results;
  private Map<String, ?> value;
  private int index;
  private AuthenticatorCredentials credentials;
  private PartnerConnection partnerConnection;
  private SObjectDescriptor sObjectDescriptor;
  private List<String> fieldsNames;
  private String fields;
  private String sObjectName;

  public SalesforceWideRecordReader(Schema schema, String query, SoapRecordToMapTransformer transformer) {
    super(schema);
    this.query = query;
    this.transformer = transformer;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException,
    InterruptedException {
    Configuration conf = taskAttemptContext.getConfiguration();
    credentials = SalesforceConnectionUtil.getAuthenticatorCredentials(conf);
    initialize(inputSplit, credentials);
  }

  public SalesforceWideRecordReader initialize(
      InputSplit inputSplit, AuthenticatorCredentials credentials)
      throws IOException, InterruptedException {
    // Use default configurations of BulkRecordReader.
    super.initialize(inputSplit, credentials);

    this.credentials = credentials;

    List<Map<String, ?>> fetchedIdList = fetchBulkQueryIds();
    LOG.debug("Number of records received from batch job for wide object: '{}'", fetchedIdList.size());

    List<List<Map<String, ?>>> partitions =
      Lists.partition(fetchedIdList, SalesforceSourceConstants.WIDE_QUERY_MAX_BATCH_COUNT);
    LOG.debug("Number of partitions to be fetched for wide object: '{}'", partitions.size());
    try {
      partnerConnection = SalesforceConnectionUtil.getPartnerConnection(credentials);
        sObjectDescriptor = SObjectDescriptor.fromQuery(query);
      fieldsNames = sObjectDescriptor.getFieldsNames();
      fields = String.join(",", fieldsNames);
      sObjectName = sObjectDescriptor.getName();
      LOG.debug("Number of partitions to be fetched for wide object: '{}'", partitions.size());
    } catch (ConnectionException e) {
      String errorMessage = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      throw new RuntimeException(
        String.format(
          "Failed to create a Salesforce SOAP connection during the init for reads: %s",
          errorMessage),
        e);
    }

    batchIterator = partitions.iterator();

    // initialize the 1st batch
    results = fetchBatchRecords();

    return this;
  }

  public List<Map<String, ?>> fetchBatchRecords() {
    List<Map<String, ?>> idList = batchIterator.next();
    LOG.info("Fetching batch of {} records from {}.", idList.size(), sObjectName);
    return processPartition(partnerConnection, fields, sObjectName, idList, sObjectDescriptor);
  }

  @Override
  public boolean nextKeyValue() {
    // If all current results are consumed, try to load the next batch
    if (index >= results.size()) {
      if (!batchIterator.hasNext()) {
        return false;
      }

      results = fetchBatchRecords();
      index = 0;

      if (results.isEmpty()) {
        return false;
      }
    }
    value = results.get(index++);
    return true;
  }

  @Override
  public Map<String, ?> getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() {
    return results == null || results.isEmpty() ? 0.0f : (float) index / results.size();
  }

  /**
   * Fetches single entry map (Id -> SObjectId_value) values received from Bulk API.
   *
   * @throws IOException          can be due error during reading query
   */
  private List<Map<String, ?>> fetchBulkQueryIds()
    throws IOException {
    List<Map<String, ?>> fetchedIdList = new ArrayList<>();
    while (super.nextKeyValue()) {
      fetchedIdList.add(super.getCurrentValue());
    }
    return fetchedIdList;
  }

  /**
   * Transforms list of single entry map to array of SObject ids.
   * <p/>
   * Example:
   * <ul>
   *  <li>Expected list of map format: `List(Map(Id -> SObject_id1), Map(Id -> SObject_id2), ...)`</li>
   *  <li>Result array: `[SObject_id1, SObject_id2, ...]`</li>
   * </ul>
   *
   * @param subIds list of single entry Map
   * @return array of SObject ids
   */
  private String[] getSObjectIds(List<Map<String, ?>> subIds) {
    return subIds.stream()
      .map(Map::values)
      .flatMap(Collection::stream)
      .toArray(String[]::new);
  }

  /**
   * Fetches wide object records through SOAP API.
   *
   * @param partnerConnection SOAP connection
   * @param fields            SObject fields to be fetched
   * @param sObjectName       SObject name
   * @param sObjectIds        SObject ids to be fetched
   * @return fetched SObject array
   */
  private SObject[] fetchPartition(PartnerConnection partnerConnection, String fields, String sObjectName,
                                   String[] sObjectIds) {
    try {
      return partnerConnection.retrieve(fields, sObjectName, sObjectIds);
    } catch (ConnectionException e) {
      LOG.trace("Fetched SObject name: '{}', fields: '{}', Ids: '{}'", sObjectName, fields,
                String.join(",", sObjectIds));
      String errorMessage = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      throw new RuntimeException(
        String.format("Failed to retrieve data for SObject '%s': %s",
                      sObjectName, errorMessage),
        e);
    }
  }

  /**
   * Processes a partition of SObject records by dividing the IDs into smaller batches,
   * retrieving the corresponding records from Salesforce, and transforming them into maps.
   *
   * @param partnerConnection the Salesforce partner connection used for retrieving data.
   * @param fields the fields to be retrieved for each SObject.
   * @param sObjectName the name of the Salesforce object (e.g., Account, Lead).
   * @param partition the partition containing the ID records to be processed.
   * @param sObjectDescriptor descriptor containing the structure of the SObject.
   * @return result from partitions
   */
  private List<Map<String, ?>> processPartition(PartnerConnection partnerConnection, String fields, String sObjectName,
                                                List<Map<String, ?>> partition, SObjectDescriptor sObjectDescriptor) {
    List<Map<String, ?>> partitionResults = new ArrayList<>();
     // Divide the list of SObject Ids into smaller batches to avoid exceeding retrieve id limits.

    /* see more - https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/
    salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_apicalls.htm */
    List<List<String>> idBatches = Lists.partition(
        Arrays.asList(getSObjectIds(partition)), SalesforceSourceConstants.RETRIEVE_MAX_BATCH_COUNT);

    // Iterate over each batch of Ids to fetch the records.
    idBatches.forEach(idBatch -> {
      SObject[] fetchedObjects = fetchPartition(
          partnerConnection, fields, sObjectName, idBatch.toArray(new String[0]));
      Arrays.stream(fetchedObjects)
          .map(sObject -> transformer.transformToMap(sObject, sObjectDescriptor))
          .forEach(partitionResults::add);
    });

    return partitionResults;
  }
}

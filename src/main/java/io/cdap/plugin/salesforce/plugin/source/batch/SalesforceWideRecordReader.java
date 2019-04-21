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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.bind.XmlObject;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * RecordReader implementation for wide SOQL queries. Reads a single Salesforce batch of SObject Id's from bulk job
 * provided in InputSplit, creates subpartitions and makes parallel SOAP calls to retrieve all values.
 */
public class SalesforceWideRecordReader extends SalesforceRecordReader {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceWideRecordReader.class);
  private static final ObjectWriter JSON_WRITER = new ObjectMapper().writer();

  private final String query;

  private List<Map<String, String>> results;
  private Map<String, String> value;
  private int index;

  public SalesforceWideRecordReader(Schema schema, String query) {
    super(schema);
    this.query = query;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException,
    InterruptedException {
    List<Map<String, String>> fetchedIdList = fetchBulkQueryIds(inputSplit, taskAttemptContext);
    LOG.debug("Number of records received from batch job for wide object: '{}'", fetchedIdList.size());

    Configuration conf = taskAttemptContext.getConfiguration();
    try {
      AuthenticatorCredentials credentials = SalesforceConnectionUtil.getAuthenticatorCredentials(conf);
      PartnerConnection partnerConnection = SalesforceConnectionUtil.getPartnerConnection(credentials);

      SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(query);
      List<String> fieldsNames = sObjectDescriptor.getFieldsNames();
      String fields = String.join(",", fieldsNames);
      String sObjectName = sObjectDescriptor.getName();

      List<List<Map<String, String>>> partitions =
        Lists.partition(fetchedIdList, SalesforceSourceConstants.WIDE_QUERY_MAX_BATCH_COUNT);
      LOG.debug("Number of partitions to be fetched for wide object: '{}'", partitions.size());

      results = partitions.parallelStream()
        .map(this::getSObjectIds)
        .map(sObjectIds -> fetchPartition(partnerConnection, fields, sObjectName, sObjectIds))
        .flatMap(Arrays::stream)
        .map(sObject -> transformToMap(sObject, sObjectDescriptor.getFields()))
        .collect(Collectors.toList());
    } catch (ConnectionException e) {
      throw new RuntimeException("Cannot create Salesforce SOAP connection", e);
    }
  }

  @Override
  public boolean nextKeyValue() {
    if (results.size() == index) {
      return false;
    }
    value = results.get(index++);
    return true;
  }

  @Override
  public Map<String, String> getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() {
    return results == null || results.isEmpty() ? 0.0f : (float) index / results.size();
  }

  @VisibleForTesting
  Map<String, String> transformToMap(SObject sObject, List<SObjectDescriptor.FieldDescriptor> fieldsNames) {
    Map<String, String> result = new HashMap<>(fieldsNames.size());
    for (SObjectDescriptor.FieldDescriptor fieldDescriptor : fieldsNames) {
      Object fieldValue = extractValue(sObject, fieldDescriptor.getName(), fieldDescriptor.getParents());

      result.put(fieldDescriptor.getFullName(), fieldValue == null ? null : String.valueOf(fieldValue));
    }
    return result;
  }

  /**
   * Fetches single entry map (Id -> SObjectId_value) values received from Bulk API.
   *
   * @param inputSplit         specifies batch details
   * @param taskAttemptContext task context
   * @return list of single entry Map
   * @throws IOException          can be due error during reading query
   * @throws InterruptedException interrupted sleep while waiting for batch results
   */
  private List<Map<String, String>> fetchBulkQueryIds(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    super.initialize(inputSplit, taskAttemptContext);
    List<Map<String, String>> fetchedIdList = new ArrayList<>();
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
   * @param subIds list of single entry Map
   * @return array of SObject ids
   */
  private String[] getSObjectIds(List<Map<String, String>> subIds) {
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
      throw new RuntimeException(String.format("Cannot retrieve data for SObject '%s'", sObjectName), e);
    }
  }

  /**
   * Extracts value from XmlObject field. Reference type fields extracted recursively.
   * <p/>
   * Example: `SELECT Id, Name, Campaign.Id FROM Opportunity LIMIT 1`
   * <p/>
   * Response:
   * <pre>
   * XmlObject{name=Opportunity, value=null,
   *   children=[
   *     XmlObject{name=Id, value=oid-1, children=[]},
   *     XmlObject{name=Name, value=value1, children=[]},
   *     XmlObject{name=Campaign, value=null, children=[XmlObject{name=Id, value=cid-1, children=[]}]}
   *   ]
   * }
   * </pre>
   * <ul>
   *  <li>Extract simple field  `Id` from SObject Opportunity:
   *  name=`Id`, children=`empty List()` -> `oid-1`</li>
   *  <li>Extract simple field  `Name` from SObject Opportunity:
   *  name=`Name`, children=`empty List()` -> `value1`</li>
   *  <li>Extract reference field  `Campaign.Id` from SObject Opportunity:
   *  name=`Id`, children=`List(Campaign)` -> `cid-1`</li>
   * </ul>
   *
   * @param xmlObject field value holder
   * @param name      field name
   * @param children  field's children names
   * @return field value
   */
  private Object extractValue(XmlObject xmlObject, String name, List<String> children) {
    if (children.isEmpty()) {
      Object value = xmlObject.getField(name);
      if (value instanceof XmlObject) {
        value = extractCompoundValue(name, (XmlObject) value);
      }
      return value;
    }
    String childName = children.get(0);
    XmlObject child = xmlObject.getChild(childName);
    if (child == null) {
      throw new IllegalStateException(
        String.format("SObject reference field with name '%s' not found in parent '%s'",
                      childName, xmlObject.getName().getLocalPart()));
    }
    // remove higher level child from list and check remaining
    return extractValue(child, name, children.subList(1, children.size()));
  }

  /**
   * Extracts compound values and retrieves value in JSON format.
   *
   * @param name field name
   * @param compoundField compound field value holder
   * @return compound value
   */
  private String extractCompoundValue(String name, XmlObject compoundField) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (Iterator<XmlObject> it = compoundField.getChildren(); it.hasNext(); ) {
      XmlObject compoundChild = it.next();
      map.put(compoundChild.getName().getLocalPart(), compoundChild.getValue());
    }
    try {
      return JSON_WRITER.writeValueAsString(map);
    } catch (IOException e) {
      throw new RuntimeException(
        String.format("Cannot transform compound value for field '%s'. Compound value '%s'", name, compoundField), e);
    }
  }
}

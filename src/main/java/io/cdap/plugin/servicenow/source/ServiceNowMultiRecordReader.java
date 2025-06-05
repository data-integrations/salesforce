/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.servicenow.source;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.servicenow.apiclient.ServiceNowAPIException;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.connector.ServiceNowRecordConverter;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Record reader that reads the entire contents of a ServiceNow table.
 */
public class ServiceNowMultiRecordReader extends ServiceNowBaseRecordReader {

  private final ServiceNowMultiSourceConfig multiSourcePluginConf;
  private ServiceNowTableAPIClientImpl restApi;

  ServiceNowMultiRecordReader(ServiceNowMultiSourceConfig multiSourcePluginConf) {
    super();
    this.multiSourcePluginConf = multiSourcePluginConf;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) {
    this.split = (ServiceNowInputSplit) split;
    this.pos = 0;
    restApi = new ServiceNowTableAPIClientImpl(multiSourcePluginConf.getConnection());
    tableName = ((ServiceNowInputSplit) split).getTableName();
    tableNameField = multiSourcePluginConf.getTableNameField();
    fetchSchema(restApi);
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    try {
      if (results == null) {
        fetchData();
      }

      if (!iterator.hasNext()) {
        return false;
      }

      row = iterator.next();

      pos++;
    } catch (Exception e) {
      throw new IOException("Exception in nextKeyValue", e);
    }
    return true;
  }

  @Override
  public StructuredRecord getCurrentValue() throws IOException {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    recordBuilder.set(tableNameField, tableName);

    try {
      for (Schema.Field field : tableFields) {
        String fieldName = field.getName();
        ServiceNowRecordConverter.convertToValue(fieldName, field.getSchema(), row,
                                                 recordBuilder);
      }
    } catch (Exception e) {
      throw new IOException("Error decoding row from table " + tableName, e);
    }
    return recordBuilder.build();
  }

  @VisibleForTesting
  void fetchData() throws ServiceNowAPIException {
    // Get the table data
    results = restApi.fetchTableRecordsRetryableMode(tableName, multiSourcePluginConf.getValueType(),
                                                     multiSourcePluginConf.getStartDate(),
                                                     multiSourcePluginConf.getEndDate(), split.getOffset(),
                                                     multiSourcePluginConf.getPageSize());

    iterator = results.iterator();
  }

  private void fetchSchema(ServiceNowTableAPIClientImpl restApi) {
    // Fetch the schema
    try {
      Schema tempSchema = restApi.fetchTableSchema(tableName, multiSourcePluginConf.getValueType());
      tableFields = tempSchema.getFields();
      List<Schema.Field> schemaFields = new ArrayList<>(tableFields);
      schemaFields.add(Schema.Field.of(tableNameField, Schema.of(Schema.Type.STRING)));
      schema = Schema.recordOf(tableName, schemaFields);
    } catch (ServiceNowAPIException e) {
      throw new RuntimeException(e);
    }
  }

}

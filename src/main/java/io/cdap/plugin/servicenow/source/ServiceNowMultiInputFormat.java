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

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.servicenow.apiclient.ServiceNowAPIException;
import io.cdap.plugin.servicenow.apiclient.ServiceNowTableAPIClientImpl;
import io.cdap.plugin.servicenow.connector.ServiceNowConnectorConfig;
import io.cdap.plugin.servicenow.util.ServiceNowConstants;
import io.cdap.plugin.servicenow.util.ServiceNowTableInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ServiceNow input format.
 */
public class ServiceNowMultiInputFormat extends InputFormat<NullWritable, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceNowMultiInputFormat.class);

  /**
   * Updates the jobConfig with the ServiceNow table information, which will then be read in getSplit() function.
   *
   * @param jobConfig the job configuration
   * @param conf      the database conf
   * @return Collection of ServiceNowTableInfo containing table and schema.
   */
  public static Set<ServiceNowTableInfo> setInput(Configuration jobConfig,
                                                  ServiceNowMultiSourceConfig conf) {
    ServiceNowJobConfiguration jobConf = new ServiceNowJobConfiguration(jobConfig);
    jobConf.setMultiSourcePluginConfiguration(conf);

    // Depending on conf value fetch the list of fields for each table and create schema object
    // return the schema object for each table as ServiceNowTableInfo
    Set<ServiceNowTableInfo> tableInfos = fetchTablesInfo(conf.getConnection(), conf.getTableNames());

    jobConf.setTableInfos(tableInfos.stream().collect(Collectors.toList()));

    return tableInfos;
  }

  static Set<ServiceNowTableInfo> fetchTablesInfo(ServiceNowConnectorConfig conf, String tableNames) {

    Set<ServiceNowTableInfo> tablesInfos = new LinkedHashSet<>();

    Set<String> tableNameSet = getList(tableNames);
    for (String table : tableNameSet) {
      ServiceNowTableInfo tableInfo = getTableMetaData(table, conf);
      if (tableInfo == null) {
        continue;
      }
      tablesInfos.add(tableInfo);
    }

    return tablesInfos;
  }

  private static ServiceNowTableInfo getTableMetaData(String tableName, ServiceNowConnectorConfig conf) {
    // Call API to fetch first record from the table
    ServiceNowTableAPIClientImpl restApi = new ServiceNowTableAPIClientImpl(conf);

    Schema schema;
    int recordCount;
    try {
      schema = restApi.fetchTableSchema(tableName);
      recordCount = restApi.getTableRecordCount(tableName);
    } catch (ServiceNowAPIException e) {
      throw new RuntimeException(e);
    }
    LOG.debug("table {}, rows = {}", tableName, recordCount);
    return new ServiceNowTableInfo(tableName, schema, recordCount);
  }

  public static Set<String> getList(String value) {
    return Strings.isNullOrEmpty(value)
      ? Collections.emptySet()
      : Stream.of(value.split(","))
      .map(String::trim)
      .filter(name -> !name.isEmpty())
      .collect(Collectors.toSet());
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    ServiceNowJobConfiguration jobConfig = new ServiceNowJobConfiguration(jobContext.getConfiguration());
    int pageSize = jobConfig.getPluginConf().getPageSize().intValue();
    List<ServiceNowTableInfo> tableInfos = jobConfig.getTableInfos();
    List<InputSplit> resultSplits = new ArrayList<>();

    for (ServiceNowTableInfo tableInfo : tableInfos) {
      String tableName = tableInfo.getTableName();
      int totalRecords = tableInfo.getRecordCount();

      int pages = (totalRecords / pageSize);
      if (totalRecords % pageSize > 0) {
        pages++;
      }
      int offset = 0;

      for (int page = 1; page <= pages; page++) {
        resultSplits.add(new ServiceNowInputSplit(tableName, offset));
        offset += pageSize;
      }
    }

    return resultSplits;
  }

  @Override
  public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit inputSplit,
                                                                         TaskAttemptContext taskAttemptContext) {
    ServiceNowJobConfiguration jobConfig = new ServiceNowJobConfiguration(taskAttemptContext.getConfiguration());
    ServiceNowMultiSourceConfig pluginConf = jobConfig.getMultiSourcePluginConf();

    return new ServiceNowMultiRecordReader(pluginConf);
  }
}

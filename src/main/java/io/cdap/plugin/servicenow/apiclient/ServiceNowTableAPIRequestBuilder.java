/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.servicenow.apiclient;

import com.google.common.base.Joiner;
import io.cdap.plugin.servicenow.restapi.RestAPIRequest;
import io.cdap.plugin.servicenow.util.SourceValueType;

import java.net.URLEncoder;
import java.util.Arrays;

/**
 * ServiceNowTableAPIRequestBuilder.
 */
public class ServiceNowTableAPIRequestBuilder extends RestAPIRequest.Builder {

  /**
   * ServiceNow API URL to fetch table records
   */
  private static final String TABLE_API_URL_TEMPLATE = "%s/api/now/table/%s";

  /**
   * ServiceNow API URL to insert/update records into the table
   */
  private static final String BATCH_API_URL_TEMPLATE = "%s/api/now/v1/batch";

  /**
   * ServiceNow API URL to fetch table metadata
   */
  private static final String SCHEMA_API_URL_TEMPLATE = "%s/api/now/doc/table/schema/%s";

  /**
   * ServiceNow API URL to fetch column metadata
   */
  private static final String METADATA_API_URL_TEMPLATE = "%s/api/now/ui/meta/%s";

  public ServiceNowTableAPIRequestBuilder(String instanceBaseUrl, String tableName, boolean isSchemaRequired) {
    if (isSchemaRequired) {
      this.setUrl(String.format(METADATA_API_URL_TEMPLATE, instanceBaseUrl, tableName));
    } else {
      this.setUrl(String.format(TABLE_API_URL_TEMPLATE, instanceBaseUrl, tableName));
    }
  }

  public ServiceNowTableAPIRequestBuilder(String instanceBaseUrl) {
    this.setUrl(String.format(BATCH_API_URL_TEMPLATE, instanceBaseUrl));
  }

  /**
   * Sets the filter query for ServiceNow Rest API request.
   *
   * @param query the filter query for ServiceNow Rest API request.
   * @return
   */
  public ServiceNowTableAPIRequestBuilder setQuery(String query) {
    try {
      this.parameters.put("sysparm_query", URLEncoder.encode(query, "UTF-8"));
    } catch (Exception e) {
    }
    return this;
  }

  public ServiceNowTableAPIRequestBuilder setOffset(int offset) {
    this.parameters.put("sysparm_offset", String.valueOf(offset));
    return this;
  }

  public ServiceNowTableAPIRequestBuilder setLimit(int limit) {
    this.parameters.put("sysparm_limit", String.valueOf(limit));
    return this;
  }

  /**
   * Sets the list of fields to be added in the JSON response.
   *
   * @param fields The list of fields to be added in the JSON response
   * @return
   */
  public ServiceNowTableAPIRequestBuilder setFields(String... fields) {
    if (fields == null || fields.length == 0) {
      return this;
    }

    try {
      this.parameters.put("sysparm_fields", URLEncoder.encode(Joiner.on(',').join(Arrays.asList(fields)), "UTF-8"));
    } catch (Exception e) {
    }
    return this;
  }

  public ServiceNowTableAPIRequestBuilder setDisplayValue(SourceValueType displayValue) {
    this.parameters.put("sysparm_display_value", displayValue.getValue());
    return this;
  }

  public ServiceNowTableAPIRequestBuilder setExcludeReferenceLink(boolean excludeRefLink) {
    this.parameters.put("sysparm_exclude_reference_link", String.valueOf(excludeRefLink));
    return this;
  }
}

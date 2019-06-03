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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.SalesforceTransformUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Transforms a {@link StructuredRecord} to a {@link CSVRecord}
 */
public class StructuredRecordToCSVRecordTransformer {

  public CSVRecord transform(StructuredRecord record) {
    List<String> fieldNames = new ArrayList<>();
    List<String> values = new ArrayList<>();

    for (Schema.Field field : record.getSchema().getFields()) {
      String fieldName = field.getName();
      String value = SalesforceTransformUtil.convertSchemaFieldToString(record.get(fieldName), field);

      fieldNames.add(fieldName);
      values.add(value);
    }

    return new CSVRecord(fieldNames, values);
  }
}

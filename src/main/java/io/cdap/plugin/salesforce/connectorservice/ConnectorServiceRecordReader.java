/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.salesforce.connectorservice;

import com.google.cloud.connector.api.data.RecordReader;
import com.google.cloud.datafusion.api.plugin.data.RecordBuilder;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.plugin.source.batch.MapToRecordTransformer;
import java.io.IOException;
import java.util.Map;

/** A record reader */
public class ConnectorServiceRecordReader implements RecordReader {
  private final org.apache.hadoop.mapreduce.RecordReader<Schema, Map<String, ?>> delegate;
  private final MapToRecordTransformer transformer;

  public ConnectorServiceRecordReader(
      org.apache.hadoop.mapreduce.RecordReader<Schema, Map<String, ?>> delegate) {
    this.delegate = delegate;
    this.transformer = new MapToRecordTransformer();
  }

  @Override
  public boolean buildNextRecord(RecordBuilder recordBuilder) {
    try {
      if (delegate.nextKeyValue()) {
        transformer.transform(delegate.getCurrentKey(), delegate.getCurrentValue(), recordBuilder);
        return true;
      }
      return false;
    } catch (Exception e) {
      throw new RuntimeException("Failed to read next record", e);
    }
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}

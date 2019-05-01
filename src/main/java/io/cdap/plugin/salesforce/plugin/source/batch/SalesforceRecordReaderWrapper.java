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

import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Record reader which delegates all reader work to the input record reader.
 * For each current key returns current SObject schema,
 * for each current value adds new field with SObject name to which value belongs to
 * if {@link #sObjectNameField} is set.
 */
public class SalesforceRecordReaderWrapper extends RecordReader<Schema, Map<String, String>> {

  private final String sObjectName;
  private final String sObjectNameField;
  private final RecordReader<Schema, Map<String, String>> delegate;

  public SalesforceRecordReaderWrapper(String sObjectName,
                                       @Nullable String sObjectNameField,
                                       RecordReader<Schema, Map<String, String>> delegate) {
    this.sObjectName = sObjectName;
    this.sObjectNameField = sObjectNameField;
    this.delegate = delegate;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    delegate.initialize(split, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return delegate.nextKeyValue();
  }

  @Override
  public Schema getCurrentKey() throws IOException, InterruptedException {
    return delegate.getCurrentKey();
  }

  @Override
  public Map<String, String> getCurrentValue() throws IOException, InterruptedException {
    Map<String, String> currentValue = delegate.getCurrentValue();
    if (sObjectNameField == null) {
      return currentValue;
    }
    Map<String, String> updatedCurrentValue = new HashMap<>(currentValue);
    updatedCurrentValue.put(sObjectNameField, sObjectName);
    return updatedCurrentValue;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return delegate.getProgress();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}

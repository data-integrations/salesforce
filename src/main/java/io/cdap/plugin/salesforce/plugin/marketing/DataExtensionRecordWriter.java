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

package io.cdap.plugin.salesforce.plugin.marketing;

import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETResult;
import com.exacttarget.fuelsdk.ETSdkException;
import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes records to Salesforce Marketing Cloud Data Extensions.
 */
public class DataExtensionRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(DataExtensionRecordWriter.class);
  private final DataExtensionClient client;
  private final List<StructuredRecord> batch;
  private final int maxBatchSize;
  private final boolean failOnError;
  private final Operation operation;

  public DataExtensionRecordWriter(DataExtensionClient client, Operation operation,
                                   int maxBatchSize, boolean failOnError) {
    this.client = client;
    this.operation = operation;
    this.maxBatchSize = maxBatchSize;
    this.failOnError = failOnError;
    this.batch = new ArrayList<>(maxBatchSize);
  }

  @Override
  public void write(NullWritable key, StructuredRecord value) throws IOException {
    if (batch.size() >= maxBatchSize) {
      writeBatch();
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException {
    writeBatch();
  }

  private void writeBatch() throws IOException {
    try {
      boolean failed = false;
      List<ETResult<ETDataExtensionRow>> results;
      switch (operation) {
        case INSERT:
          results = client.insert(batch);
          break;
        case UPDATE:
          results = client.update(batch);
          break;
        default:
          // should never happen
          throw new IllegalStateException("Unsupported operation " + operation);
      }
      for (ETResult<ETDataExtensionRow> result : results) {
        if (result.getStatus() == ETResult.Status.ERROR) {
          failed = true;
          String errorMessage = String.format("Failed to write record %s: %s", result.getObject().getColumns(),
                                              result.getErrorMessage());
          if (failOnError) {
            LOG.error(errorMessage);
          } else {
            LOG.warn(errorMessage);
          }
        }
      }
      batch.clear();
      if (failed && failOnError) {
        throw new IOException(String.format("Failed to write records to data extension '%s'",
                                            client.getDataExtensionKey()));
      }
    } catch (ETSdkException e) {
      throw new IOException(e.getMessage(), e);
    }
  }
}

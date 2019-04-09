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

package io.cdap.salesforce.plugin.source.batch;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A split used for mapreduce
 */
public class SalesforceSplit extends InputSplit implements Writable {
  private String jobId;
  private String batchId;

  @SuppressWarnings("unused")
  public SalesforceSplit() {
    // For serialization
  }

  public SalesforceSplit(String jobId, String batchId) {
    this.jobId = jobId;
    this.batchId = batchId;
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    jobId = dataInput.readUTF();
    batchId = dataInput.readUTF();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(jobId);
    dataOutput.writeUTF(batchId);
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }

  public String getJobId() {
    return jobId;
  }

  public String getBatchId() {
    return batchId;
  }
}

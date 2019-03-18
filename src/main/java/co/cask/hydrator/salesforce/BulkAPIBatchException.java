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


package co.cask.hydrator.salesforce;

import com.sforce.async.BatchInfo;

/**
 * Exception is thrown when a bulk api batch fails or timeouts.
 */
public class BulkAPIBatchException extends RuntimeException {
  private BatchInfo batchInfo;

  public BulkAPIBatchException(String message, BatchInfo batchInfo) {
    super(String.format("%s. BatchId='%s', Reason='%s'", message, batchInfo.getId(), batchInfo.getStateMessage()));
    this.batchInfo = batchInfo;
  }

  public BatchInfo getBatchInfo() {
    return batchInfo;
  }
}

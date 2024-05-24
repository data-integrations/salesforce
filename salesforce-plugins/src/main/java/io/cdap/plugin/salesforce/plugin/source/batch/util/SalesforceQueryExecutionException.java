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

package io.cdap.plugin.salesforce.plugin.source.batch.util;

/**
 * Custom exception class for handling errors related to Salesforce Query execution.
 * This exception should be thrown when an issue occurs during the execution of a Salesforce bulk query,
 * and the calling code should consider retrying the operation.
 */
public class SalesforceQueryExecutionException extends Exception {
  /**
   * Constructs a new SalesforceQueryExecutionException with the specified detail message.
   *
   * @param message The detail message that describes the exception.
   */
  public SalesforceQueryExecutionException(String message) {
    super(message);
  }

  public SalesforceQueryExecutionException(Throwable cause) {
    super(cause);
  }
}



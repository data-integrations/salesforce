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
package io.cdap.plugin.salesforce.utils;

import com.sforce.soap.partner.fault.IApiFault;

/**
 * Utility class which provide exception handling from Salesforce.
 */
public class SalesforceErrorUtil {

  /**
   * @param e Exception thrown from salesforce APIs
   * @return  error message sent by APIs.
   */
  public static String getSalesforceErrorMessageFromException(Exception e) {
    if (e instanceof IApiFault) {
      return ((IApiFault) e).getExceptionMessage();
    } else {
      return e.getMessage();
    }
  }
}

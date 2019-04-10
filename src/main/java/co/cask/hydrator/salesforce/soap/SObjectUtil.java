/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

package co.cask.hydrator.salesforce.soap;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class which provides functions to work with sobjects
 */
public class SObjectUtil {
  /**
   * Create given sObjects and handle the errors returned from API
   *
   * @param partnerConnection connection so Salesforce API
   * @param sObjects an array of sObjects to create
   * @return API responses
   * @throws ConnectionException occurs due to failure to connect to Salesforce API
   */
  public static SaveResult[] createSObjects(PartnerConnection partnerConnection, SObject[] sObjects)
    throws ConnectionException {

    SaveResult[] results = partnerConnection.create(sObjects);

    for (SaveResult saveResult : results) {
      if (!saveResult.getSuccess()) {
        String allErrors = Stream.of(saveResult.getErrors())
          .map(result -> result.getMessage())
          .collect(Collectors.joining("\n"));

        throw new RuntimeException(allErrors);
      }
    }
    return results;
  }
}

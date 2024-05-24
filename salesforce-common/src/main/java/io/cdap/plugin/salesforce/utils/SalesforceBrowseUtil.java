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

import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseEntityPropertyValue;
import io.cdap.plugin.salesforce.auth.AuthenticatorCredentials;

import java.io.IOException;

/**
 * Utility class for Salesforce Browse function.
 */
public class SalesforceBrowseUtil {
  private static final String ENTITY_TYPE_OBJECTS = "object";
  private static final String LABEL_NAME = "label";

  /**
   * Browse functionality based on the config.
   *
   * @param credentials AuthenticatorCredentials to use for the call to Salesforce API.
   * @param onlyReturnQueryableObjects Whether to return only the queryable sObjects.
   * @return BrowseDetail for the given config
   * @throws IOException In case of Salesforce connection failure while browsing
   */
  public static BrowseDetail browse(AuthenticatorCredentials credentials,
                             boolean onlyReturnQueryableObjects) throws IOException {
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    int count = 0;
    try {
      PartnerConnection partnerConnection = SalesforceConnectionUtil.getPartnerConnection(credentials);
      DescribeGlobalResult dgr = partnerConnection.describeGlobal();
      // Loop through the array to get all the objects.
      for (int i = 0; i < dgr.getSobjects().length; i++) {
        String name = dgr.getSobjects()[i].getName();
        String label = dgr.getSobjects()[i].getLabel();
        boolean isQueryable = dgr.getSobjects()[i].isQueryable();

        // Continue in case of returning only queryable sObjects and the current sObject is non-queryable.
        if (onlyReturnQueryableObjects && !isQueryable) {
          continue;
        }

        BrowseEntity.Builder entity = (BrowseEntity.builder(name, name, ENTITY_TYPE_OBJECTS).
            canBrowse(false).canSample(true));
        entity.addProperty(LABEL_NAME, BrowseEntityPropertyValue.builder(label, BrowseEntityPropertyValue.
            PropertyType.STRING).build());
        browseDetailBuilder.addEntity(entity.build());
        count++;
      }
    } catch (ConnectionException e) {
      String message = SalesforceErrorUtil.getSalesforceErrorMessageFromException(e);
      throw new IOException(String.format("Cannot establish connection to Salesforce with error: %s", message), e);
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }
}

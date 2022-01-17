/*
 * Copyright © 2022 Cask Data, Inc.
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
package io.cdap.plugin.salesforce.authenticator;

import org.junit.Assert;
import org.junit.Test;

public class AuthResponseTest {

  AuthResponse authResponse;

  @Test
  public void testAuthResponse() {
    authResponse =
      new AuthResponse("token", "https://login.salesforce.com/services/oauth2/token",
                       "id", "type", "2022-01-05",
                       "Signature", "An error occurred");
    Assert.assertEquals("token", authResponse.getAccessToken());
    Assert.assertEquals("An error occurred", authResponse.getError());
    Assert.assertEquals("id", authResponse.getId());
    Assert.assertEquals("https://login.salesforce.com/services/oauth2/token", authResponse.getInstanceUrl());
    Assert.assertEquals("2022-01-05", authResponse.getIssuedAt());
    Assert.assertEquals("Signature", authResponse.getSignature());
    Assert.assertEquals("type", authResponse.getTokenType());
  }

}

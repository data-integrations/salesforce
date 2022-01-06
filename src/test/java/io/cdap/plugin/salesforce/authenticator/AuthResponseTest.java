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
package io.cdap.plugin.salesforce.authenticator;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AuthResponseTest {

  AuthResponse authResponse;

  @Before
  public void setup() {
    authResponse =
      new AuthResponse("token", "https://login.salesforce.com/services/oauth2/token", "id", "type", "2022-01-05",
        "Signature", "An error occurred");
  }

  @Test
  public void testAuthResponse() {
    assertEquals("token", authResponse.getAccessToken());
    assertEquals("An error occurred", authResponse.getError());
    assertEquals("id", authResponse.getId());
    assertEquals("https://login.salesforce.com/services/oauth2/token", authResponse.getInstanceUrl());
    assertEquals("2022-01-05", authResponse.getIssuedAt());
    assertEquals("Signature", authResponse.getSignature());
    assertEquals("type", authResponse.getTokenType());
  }

}


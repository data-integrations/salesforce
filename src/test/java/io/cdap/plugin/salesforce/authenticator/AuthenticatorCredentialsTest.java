/*
 * Copyright © 2024 Cask Data, Inc.
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

import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AuthenticatorCredentialsTest {
    private static final String USERNAME = "dummy_user";
    private static final String PASSWORD = "dummy_pass";
    private static final String CONSUMER_KEY = "key123";
    private static final String CONSUMER_SECRET = "secret123";
    private static final String LOGIN_URL = "login-url";
    private static final OAuthInfo O_AUTH_INFO = new OAuthInfo("dummyAccessToken", "dummyUrl");

    @Test
    public void buildFromParameters_passwordFlow() {
        AuthenticatorCredentials expected = AuthenticatorCredentials.getBuilder(
                USERNAME, PASSWORD, CONSUMER_KEY, CONSUMER_SECRET, LOGIN_URL).build();

        AuthenticatorCredentials actual = AuthenticatorCredentials.fromParameters(
                USERNAME, PASSWORD, CONSUMER_KEY, CONSUMER_SECRET, LOGIN_URL, null, null, null);

        assertEquals(expected, actual);
    }

    @Test
    public void buildFromParameters_clientCredentialsFlow() {
        AuthenticatorCredentials expected = AuthenticatorCredentials.getBuilder(
                CONSUMER_KEY, CONSUMER_SECRET, LOGIN_URL).build();

        AuthenticatorCredentials actual = AuthenticatorCredentials.fromParameters(
                null, null, CONSUMER_KEY, CONSUMER_SECRET, LOGIN_URL, null, null, null);

        assertEquals(expected, actual);
    }

    @Test
    public void buildFromParameters_OAuthFlow() {
        AuthenticatorCredentials expected = AuthenticatorCredentials.getBuilder(O_AUTH_INFO).build();

        AuthenticatorCredentials actual = AuthenticatorCredentials.fromParameters(O_AUTH_INFO, null, null, null);

        assertEquals(expected, actual);
    }
}

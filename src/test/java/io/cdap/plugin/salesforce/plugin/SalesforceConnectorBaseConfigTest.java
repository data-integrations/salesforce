/*
 * Copyright © 2026 Cask Data, Inc.
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
package io.cdap.plugin.salesforce.plugin;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.salesforce.SalesforceConstants;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests for {@link SalesforceConnectorBaseConfig#validateAuthenticationFields}.
 */
public class SalesforceConnectorBaseConfigTest {

  private static final String VALID_CONSUMER_KEY = "testConsumerKey";
  private static final String VALID_CONSUMER_SECRET = "testConsumerSecret";
  private static final String VALID_USERNAME = "testUser";
  private static final String VALID_PASSWORD = "testPassword";
  private static final String VALID_SECURITY_TOKEN = "testToken";
  private static final String VALID_LOGIN_URL = "https://login.salesforce.com/services/oauth2/token";

  // --- PASSWORD grant type tests ---

  @Test
  public void testPasswordGrantAllFieldsValid() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      VALID_CONSUMER_KEY, VALID_CONSUMER_SECRET, VALID_USERNAME, VALID_PASSWORD,
      VALID_LOGIN_URL, VALID_SECURITY_TOKEN, null, null, null, null, null, null, null, "password");
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testPasswordGrantMissingConsumerKey() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      null, VALID_CONSUMER_SECRET, VALID_USERNAME, VALID_PASSWORD,
      VALID_LOGIN_URL, VALID_SECURITY_TOKEN, null, null, null, null, null, null, null, "password");
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    assertSingleFailureOnField(collector, SalesforceConstants.PROPERTY_CONSUMER_KEY,
      "Consumer Key is required for authentication.");
  }

  @Test
  public void testPasswordGrantEmptyConsumerKey() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      "", VALID_CONSUMER_SECRET, VALID_USERNAME, VALID_PASSWORD,
      VALID_LOGIN_URL, VALID_SECURITY_TOKEN, null, null, null, null, null, null, null, "password");
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    assertSingleFailureOnField(collector, SalesforceConstants.PROPERTY_CONSUMER_KEY,
      "Consumer Key is required for authentication.");
  }

  @Test
  public void testPasswordGrantMissingConsumerSecret() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      VALID_CONSUMER_KEY, null, VALID_USERNAME, VALID_PASSWORD,
      VALID_LOGIN_URL, VALID_SECURITY_TOKEN, null, null, null, null, null, null, null, "password");
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    assertSingleFailureOnField(collector, SalesforceConstants.PROPERTY_CONSUMER_SECRET,
      "Consumer Secret is required for authentication.");
  }

  @Test
  public void testPasswordGrantMissingLoginUrl() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      VALID_CONSUMER_KEY, VALID_CONSUMER_SECRET, VALID_USERNAME, VALID_PASSWORD,
      null, VALID_SECURITY_TOKEN, null, null, null, null, null, null, null, "password");
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    assertSingleFailureOnField(collector, SalesforceConstants.PROPERTY_LOGIN_URL,
      "Login URL is required for authentication.");
  }

  @Test
  public void testPasswordGrantMissingUsername() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      VALID_CONSUMER_KEY, VALID_CONSUMER_SECRET, null, VALID_PASSWORD,
      VALID_LOGIN_URL, VALID_SECURITY_TOKEN, null, null, null, null, null, null, null, "password");
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    assertSingleFailureOnField(collector, SalesforceConstants.PROPERTY_USERNAME,
      "Username is required for password grant type authentication.");
  }

  @Test
  public void testPasswordGrantMissingPassword() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      VALID_CONSUMER_KEY, VALID_CONSUMER_SECRET, VALID_USERNAME, null,
      VALID_LOGIN_URL, VALID_SECURITY_TOKEN, null, null, null, null, null, null, null, "password");
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    assertSingleFailureOnField(collector, SalesforceConstants.PROPERTY_PASSWORD,
      "Password is required for password grant type authentication.");
  }

  @Test
  public void testPasswordGrantMissingSecurityToken() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      VALID_CONSUMER_KEY, VALID_CONSUMER_SECRET, VALID_USERNAME, VALID_PASSWORD,
      VALID_LOGIN_URL, null, null, null, null, null, null, null, null, "password");
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    assertSingleFailureOnField(collector, SalesforceConstants.PROPERTY_SECURITY_TOKEN,
      "Security Token is required for password grant type authentication.");
  }

  @Test
  public void testPasswordGrantMissingMultipleFields() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      null, null, null, null,
      null, null, null, null, null, null, null, null, null, "password");
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    List<String> failedFields = collector.getValidationFailures().stream()
      .map(f -> f.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG))
      .collect(Collectors.toList());
    Assert.assertEquals(6, collector.getValidationFailures().size());
    Assert.assertTrue(failedFields.contains(SalesforceConstants.PROPERTY_CONSUMER_KEY));
    Assert.assertTrue(failedFields.contains(SalesforceConstants.PROPERTY_CONSUMER_SECRET));
    Assert.assertTrue(failedFields.contains(SalesforceConstants.PROPERTY_LOGIN_URL));
    Assert.assertTrue(failedFields.contains(SalesforceConstants.PROPERTY_USERNAME));
    Assert.assertTrue(failedFields.contains(SalesforceConstants.PROPERTY_PASSWORD));
    Assert.assertTrue(failedFields.contains(SalesforceConstants.PROPERTY_SECURITY_TOKEN));
  }

  // --- CLIENT_CREDENTIALS grant type tests ---

  @Test
  public void testClientCredentialsGrantAllFieldsValid() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      VALID_CONSUMER_KEY, VALID_CONSUMER_SECRET, null, null,
      VALID_LOGIN_URL, null, null, null, null, null, null, null, null, "client_credentials");
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testClientCredentialsGrantMissingConsumerKey() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      null, VALID_CONSUMER_SECRET, null, null,
      VALID_LOGIN_URL, null, null, null, null, null, null, null, null, "client_credentials");
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    assertSingleFailureOnField(collector, SalesforceConstants.PROPERTY_CONSUMER_KEY,
      "Consumer Key is required for authentication.");
  }

  @Test
  public void testClientCredentialsGrantMissingConsumerSecret() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      VALID_CONSUMER_KEY, null, null, null,
      VALID_LOGIN_URL, null, null, null, null, null, null, null, null, "client_credentials");
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    assertSingleFailureOnField(collector, SalesforceConstants.PROPERTY_CONSUMER_SECRET,
      "Consumer Secret is required for authentication.");
  }

  @Test
  public void testClientCredentialsGrantMissingLoginUrl() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      VALID_CONSUMER_KEY, VALID_CONSUMER_SECRET, null, null,
      null, null, null, null, null, null, null, null, null, "client_credentials");
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    assertSingleFailureOnField(collector, SalesforceConstants.PROPERTY_LOGIN_URL,
      "Login URL is required for authentication.");
  }

  @Test
  public void testClientCredentialsGrantDoesNotRequireUsername() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      VALID_CONSUMER_KEY, VALID_CONSUMER_SECRET, null, null,
      VALID_LOGIN_URL, null, null, null, null, null, null, null, null, "client_credentials");
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    List<String> failedFields = collector.getValidationFailures().stream()
      .map(f -> f.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG))
      .collect(Collectors.toList());
    Assert.assertFalse(failedFields.contains(SalesforceConstants.PROPERTY_USERNAME));
    Assert.assertFalse(failedFields.contains(SalesforceConstants.PROPERTY_PASSWORD));
    Assert.assertFalse(failedFields.contains(SalesforceConstants.PROPERTY_SECURITY_TOKEN));
  }

  @Test
  public void testClientCredentialsGrantMissingMultipleFields() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      null, null, null, null,
      null, null, null, null, null, null, null, null, null, "client_credentials");
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    List<String> failedFields = collector.getValidationFailures().stream()
      .map(f -> f.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG))
      .collect(Collectors.toList());
    Assert.assertEquals(3, collector.getValidationFailures().size());
    Assert.assertTrue(failedFields.contains(SalesforceConstants.PROPERTY_CONSUMER_KEY));
    Assert.assertTrue(failedFields.contains(SalesforceConstants.PROPERTY_CONSUMER_SECRET));
    Assert.assertTrue(failedFields.contains(SalesforceConstants.PROPERTY_LOGIN_URL));
  }

  // --- Default grant type (null defaults to PASSWORD) ---

  @Test
  public void testDefaultGrantTypeIsPassword() {
    SalesforceConnectorBaseConfig config = new SalesforceConnectorBaseConfig(
      VALID_CONSUMER_KEY, VALID_CONSUMER_SECRET, null, null,
      VALID_LOGIN_URL, null, null, null, null, null, null, null, null, null);
    MockFailureCollector collector = new MockFailureCollector();
    config.validateAuthenticationFields(collector);
    List<String> failedFields = collector.getValidationFailures().stream()
      .map(f -> f.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG))
      .collect(Collectors.toList());
    // null grant type defaults to PASSWORD, so username/password/securityToken should be required
    Assert.assertTrue(failedFields.contains(SalesforceConstants.PROPERTY_USERNAME));
    Assert.assertTrue(failedFields.contains(SalesforceConstants.PROPERTY_PASSWORD));
    Assert.assertTrue(failedFields.contains(SalesforceConstants.PROPERTY_SECURITY_TOKEN));
  }

  private void assertSingleFailureOnField(MockFailureCollector collector, String expectedField,
                                           String expectedMessage) {
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(1, failures.size());
    Assert.assertEquals(expectedField,
      failures.get(0).getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals(expectedMessage, failures.get(0).getMessage());
  }
}

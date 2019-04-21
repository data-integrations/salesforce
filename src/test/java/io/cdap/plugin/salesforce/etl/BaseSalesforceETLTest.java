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

package io.cdap.plugin.salesforce.etl;

import com.google.common.collect.ImmutableMap;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.ErrorHandling;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Methods to run ETL with Salesforce Bulk/Streaming plugin as source, and a mock plugin as a sink.
 *
 * By default all tests will be skipped, since Salesforce credentials are needed.
 *
 * Instructions to enable the tests:
 * 1. Create/use existing Salesforce account
 * 2. Create connected application within the account to get clientId and clientSecret
 * 3. Run the tests using the command below:
 *
 * mvn clean test
 * -Dsalesforce.test.clientId= -Dsalesforce.test.clientSecret= -Dsalesforce.test.username= -Dsalesforce.test.password=
 *
 */
public abstract class BaseSalesforceETLTest extends HydratorTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(BaseSalesforceETLTest.class);

  protected static final String CLIENT_ID = System.getProperty("salesforce.test.clientId");
  protected static final String CLIENT_SECRET = System.getProperty("salesforce.test.clientSecret");
  protected static final String USERNAME = System.getProperty("salesforce.test.username");
  protected static final String PASSWORD = System.getProperty("salesforce.test.password");
  protected static final String LOGIN_URL = System.getProperty("salesforce.test.loginUrl",
                                                             "https://login.salesforce.com/services/oauth2/token");

  @Rule
  public TestName testName = new TestName();
  protected List<String> createdObjectsIds = new ArrayList<>();
  protected static PartnerConnection partnerConnection;

  @BeforeClass
  public static void initializeTests() throws ConnectionException {
    try {
      Assume.assumeNotNull(CLIENT_ID, CLIENT_SECRET, USERNAME, PASSWORD, LOGIN_URL);
    } catch (AssumptionViolatedException e) {
      LOG.warn("ETL tests are skipped. Please find the instructions on enabling it at" +
        "BaseSalesforceBatchSourceETLTest javadoc");
      throw e;
    }

    AuthenticatorCredentials credentials = SalesforceConnectionUtil.getAuthenticatorCredentials(
      USERNAME, PASSWORD, CLIENT_ID, CLIENT_SECRET, LOGIN_URL);
    partnerConnection = SalesforceConnectionUtil.getPartnerConnection(credentials);
  }

  @After
  public void cleanUpBase() throws ConnectionException {
    clearSObjects();
  }

  /**
   * Creates given sObjects and saves their IDs for deletion in the end of test run.
   *
   * @param sObjects list of sObjects to be created
   */
  protected void addSObjects(List<SObject> sObjects) {
    addSObjects(sObjects, true);
  }

  /**
   * Adds sObjects to Salesforce. Checks the result response for errors.
   * If save flag is true, saves the objects so that they can be deleted after method is run.
   *
   * @param sObjects list of sobjects to create
   * @param save if sObjects need to be saved for deletion
   */
  protected void addSObjects(List<SObject> sObjects, boolean save) {
    try {
      SaveResult[] results = partnerConnection.create(sObjects.toArray(new SObject[0]));
      if (save) {
        createdObjectsIds.addAll(Arrays.stream(results)
                                   .map(SaveResult::getId).collect(Collectors.toList()));
      }

      for (SaveResult saveResult : results) {
        if (!saveResult.getSuccess()) {
          String allErrors = Stream.of(saveResult.getErrors())
            .map(Error::getMessage)
            .collect(Collectors.joining("\n"));

          throw new RuntimeException(allErrors);
        }
      }

    } catch (ConnectionException e) {
      throw new RuntimeException("There was issue communicating with Salesforce", e);
    }
  }

  protected ImmutableMap.Builder<String, String> getBaseProperties(String referenceName) {
    return new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, referenceName)
      .put(SalesforceConstants.PROPERTY_CLIENT_ID, CLIENT_ID)
      .put(SalesforceConstants.PROPERTY_CLIENT_SECRET, CLIENT_SECRET)
      .put(SalesforceConstants.PROPERTY_USERNAME, USERNAME)
      .put(SalesforceConstants.PROPERTY_PASSWORD, PASSWORD)
      .put(SalesforceConstants.PROPERTY_LOGIN_URL, LOGIN_URL)
      .put(SalesforceConstants.PROPERTY_ERROR_HANDLING, ErrorHandling.STOP.getValue());
  }

  private void clearSObjects() throws ConnectionException {
    if (createdObjectsIds.isEmpty()) {
      return;
    }

    partnerConnection.delete(createdObjectsIds.toArray(new String[0]));
    createdObjectsIds.clear();
  }
}

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
package io.cdap.plugin.salesforce;

import com.google.common.collect.ImmutableMap;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSplitUtil;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.HttpURLConnection;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests for {@link SalesforceQueryUtil}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
    SalesforceConnectionUtil.class,
    Authenticator.class,
    SalesforceQueryUtil.class,
    OAuthInfo.class,
    HttpClient.class,
    org.eclipse.jetty.util.component.AbstractLifeCycle.class
})
public class SalesforceQueryUtilTest {

  @Test
  public void testCreateSObjectQueryWithoutFilter() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";

    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName,
                                                                 SObjectFilterDescriptor.noOp());

    Assert.assertNotNull(sObjectQuery);
    Assert.assertEquals("SELECT Id,Name,SomeField FROM sObjectName", sObjectQuery);
  }

  @Test
  public void testCreateSObjectQueryWithBlankDatetimeFilter() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";

    SObjectFilterDescriptor filterDescriptor = SObjectFilterDescriptor.interval(null, null);

    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName, filterDescriptor);

    Assert.assertNotNull(sObjectQuery);
    Assert.assertEquals("SELECT Id,Name,SomeField FROM sObjectName", sObjectQuery);
  }

  @Test
  public void testCreateSObjectQueryWithBlankDatetimeAndZeroRangeFilters() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";

    SObjectFilterDescriptor filterDescriptor = SObjectFilterDescriptor.range(
      System.currentTimeMillis(),
      Collections.singletonMap(ChronoUnit.HOURS, 0),
      Collections.singletonMap(ChronoUnit.HOURS, 0));

    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName, filterDescriptor);

    Assert.assertNotNull(sObjectQuery);
    Assert.assertEquals("SELECT Id,Name,SomeField FROM sObjectName", sObjectQuery);
  }

  @Test
  public void testCreateSObjectQueryWithDatetimeAfterFilter() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";
    SObjectFilterDescriptor filterDescriptor = SObjectFilterDescriptor.interval(
      ZonedDateTime.parse("2019-04-12T23:23:23Z", DateTimeFormatter.ISO_DATE_TIME), null);

    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName, filterDescriptor);

    Assert.assertNotNull(sObjectQuery);
    Assert.assertEquals("SELECT Id,Name,SomeField FROM sObjectName WHERE LastModifiedDate>=2019-04-12T23:23:23Z",
                        sObjectQuery);
  }

  @Test
  public void testCreateSObjectQueryWithDatetimeBeforeFilter() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";
    SObjectFilterDescriptor filterDescriptor = SObjectFilterDescriptor.interval(
      null, ZonedDateTime.parse("2019-04-22T01:01:01Z", DateTimeFormatter.ISO_DATE_TIME));

    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName, filterDescriptor);

    Assert.assertNotNull(sObjectQuery);
    Assert.assertEquals("SELECT Id,Name,SomeField FROM sObjectName WHERE LastModifiedDate<2019-04-22T01:01:01Z",
                        sObjectQuery);
  }

  @Test
  public void testCreateSObjectQueryWithDatetimeAfterAndBeforeFilters() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";
    SObjectFilterDescriptor filterDescriptor = SObjectFilterDescriptor.interval(
      ZonedDateTime.parse("2019-04-12T23:23:23Z", DateTimeFormatter.ISO_DATE_TIME),
      ZonedDateTime.parse("2019-04-22T01:01:01Z", DateTimeFormatter.ISO_DATE_TIME));

    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName, filterDescriptor);

    Assert.assertNotNull(sObjectQuery);
    Assert.assertEquals("SELECT Id,Name,SomeField FROM sObjectName WHERE " +
                          "LastModifiedDate>=2019-04-12T23:23:23Z AND LastModifiedDate<2019-04-22T01:01:01Z",
                        sObjectQuery);
  }


  @Test
  public void testCreateSObjectQueryWithDurationOnly() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    long currentTimeMillis = now.toInstant().toEpochMilli();

    SObjectFilterDescriptor filterDescriptor = SObjectFilterDescriptor.range(
      currentTimeMillis,
      Collections.singletonMap(ChronoUnit.HOURS, 6),
      Collections.singletonMap(ChronoUnit.HOURS, 0));

    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName, filterDescriptor);


    Assert.assertNotNull(sObjectQuery);
    String expected = String.format("SELECT Id,Name,SomeField "
                                      + "FROM sObjectName "
                                      + "WHERE "
                                      + "LastModifiedDate>=%s AND LastModifiedDate<%s",
                                    now.minusHours(6).format(DateTimeFormatter.ISO_DATE_TIME),
                                    now.format(DateTimeFormatter.ISO_DATE_TIME));
    Assert.assertEquals(expected, sObjectQuery);
  }

  @Test
  public void testCreateSObjectQueryWithOffsetOnly() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    long currentTimeMillis = now.toInstant().toEpochMilli();

    SObjectFilterDescriptor filterDescriptor = SObjectFilterDescriptor.range(
      currentTimeMillis,
      Collections.singletonMap(ChronoUnit.HOURS, 0),
      Collections.singletonMap(ChronoUnit.DAYS, 1));

    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName, filterDescriptor);


    Assert.assertNotNull(sObjectQuery);
    String expected = String.format("SELECT Id,Name,SomeField "
                                      + "FROM sObjectName "
                                      + "WHERE "
                                      + "LastModifiedDate<%s",
                                    now.minusDays(1).format(DateTimeFormatter.ISO_DATE_TIME));
    Assert.assertEquals(expected, sObjectQuery);
  }

  @Test
  public void testCreateSObjectQueryWithDurationAndOffset() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    long currentTimeMillis = now.toInstant().toEpochMilli();

    SObjectFilterDescriptor filterDescriptor = SObjectFilterDescriptor.range(
      currentTimeMillis,
      ImmutableMap.of(ChronoUnit.HOURS, 6, ChronoUnit.MINUTES, 10),
      Collections.singletonMap(ChronoUnit.HOURS, 1));

    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName, filterDescriptor);


    Assert.assertNotNull(sObjectQuery);
    String expected = String.format("SELECT Id,Name,SomeField "
                                      + "FROM sObjectName "
                                      + "WHERE "
                                      + "LastModifiedDate>=%s AND LastModifiedDate<%s",
                                    now.minusHours(6).minusHours(1).minusMinutes(10)
                                      .format(DateTimeFormatter.ISO_DATE_TIME),
                                    now.minusHours(1).format(DateTimeFormatter.ISO_DATE_TIME));
    Assert.assertEquals(expected, sObjectQuery);
  }

  @Test
  public void testIsQueryUnderLengthLimitTrue() {
    boolean underLengthLimit = SalesforceQueryUtil.isQueryUnderLengthLimit(
      "SELECT Id,Name,SomeField FROM sObjectName WHERE LastModifiedDate>2019-04-12T23:23:23Z");

    Assert.assertTrue(underLengthLimit);
  }

  @Test
  public void testIsQueryUnderLengthLimitFalse() {
    String fieldPrefix = "field_";
    // generate fields sequence separated by comma to exceed SOQL max length limit
    String fields = IntStream.range(0, SalesforceConstants.SOQL_MAX_LENGTH / fieldPrefix.length())
      .mapToObj(i -> fieldPrefix + i)
      .collect(Collectors.joining(","));

    boolean underLengthLimit = SalesforceQueryUtil.isQueryUnderLengthLimit(
      String.format("SELECT %s FROM sObjectName WHERE LastModifiedDate>=2019-04-12T23:23:23Z", fields));

    Assert.assertFalse(underLengthLimit);
  }

  @Test
  public void testCreateSObjectIdQuery() {
    String selectClause = "SELECT Id,Name,SomeField ";
    String fromClause = "FROM sObjectName WHERE LastModifiedDate>=2019-04-12T23:23:23Z";
    String query = selectClause + fromClause;

    String sObjectIdQuery = SalesforceQueryUtil.createSObjectIdQuery(query);

    Assert.assertEquals("SELECT Id " + fromClause, sObjectIdQuery);
  }

  @Test
  public void createCountQuery_withWhereClause_replacesSelectFieldsWithCount() {
    String query = "SELECT Id,Name,SomeField FROM sObjectName WHERE LastModifiedDate>=2019-04-12T23:23:23Z";

    String result = SalesforceQueryUtil.createCountQuery(query);

    Assert.assertEquals(
      "SELECT COUNT() FROM sObjectName WHERE LastModifiedDate>=2019-04-12T23:23:23Z",
      result);
  }

  @Test
  public void createCountQuery_withoutWhereClause_replacesSelectFieldsWithCount() {
    String query = "SELECT Id, Name FROM Account";

    String result = SalesforceQueryUtil.createCountQuery(query);

    Assert.assertEquals("SELECT COUNT() FROM Account", result);
  }

  @Test(expected = Exception.class)
  public void getQueryPlan_whenRestClientConnectionFails_throwsException() throws Exception {
    String query = "SELECT Name FROM Account";
    AuthenticatorCredentials credentials = Mockito.mock(AuthenticatorCredentials.class);
    Mockito.when(credentials.getConnectTimeout()).thenReturn(3000);
    PowerMockito.mockStatic(SalesforceConnectionUtil.class);
    PowerMockito.when(SalesforceConnectionUtil.getPartnerConnection(credentials))
        .thenThrow(new RuntimeException("REST Client Connection Failure"));

    SalesforceQueryUtil.getQueryPlan(query, credentials);
  }

  @Test
  public void getQueryPlan_success_returnsQueryPlanResponse() throws Exception {
    String query = "SELECT COUNT() FROM Account";
    AuthenticatorCredentials credentials = Mockito.mock(AuthenticatorCredentials.class);
    Mockito.when(credentials.getConnectTimeout()).thenReturn(3000);
    OAuthInfo oAuthInfo = PowerMockito.mock(OAuthInfo.class);
    Mockito.when(oAuthInfo.getInstanceURL()).thenReturn("https://instance.salesforce.com");
    Mockito.when(oAuthInfo.getAccessToken()).thenReturn("test-token");
    PowerMockito.mockStatic(Authenticator.class);
    PowerMockito.when(Authenticator.getOAuthInfo(credentials)).thenReturn(oAuthInfo);
    HttpClient httpClient = PowerMockito.mock(HttpClient.class);
    PowerMockito.whenNew(HttpClient.class).withArguments(Mockito.any(SslContextFactory.class))
        .thenReturn(httpClient);
    Request request = Mockito.mock(Request.class);
    Mockito.when(httpClient.newRequest(Mockito.anyString())).thenReturn(request);
    Mockito.when(request.method(Mockito.any(HttpMethod.class))).thenReturn(request);
    Mockito.when(request.header(Mockito.any(HttpHeader.class), Mockito.anyString()))
        .thenReturn(request);
    ContentResponse response = Mockito.mock(ContentResponse.class);
    Mockito.when(request.send()).thenReturn(response);
    Mockito.when(response.getStatus()).thenReturn(HttpURLConnection.HTTP_OK);
    Mockito.when(response.getContentAsString())
        .thenReturn("{\"plans\":[{\"relativeCost\":0.5,\"cardinality\":12345,\"leadingOperationType\":\"Index\"}]}");

    SalesforceQueryUtil.QueryPlanResponse result = SalesforceQueryUtil.getQueryPlan(query, credentials);

    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.getPlans().size());
    Assert.assertEquals(0.5, result.getPlans().get(0).getRelativeCost(), 0.0001);
    Assert.assertEquals(12345, result.getPlans().get(0).getCardinality());
    Assert.assertEquals("Index", result.getPlans().get(0).getLeadingOperationType());
  }

  @Test
  public void hasRequiredCountForPkChunking_queryPlanFails_defaultsToTrue() throws Exception {
    String query = "SELECT Id, Name FROM Opportunity";
    AuthenticatorCredentials credentials = Mockito.mock(AuthenticatorCredentials.class);
    long threshold = 100000;
    PowerMockito.mockStatic(SalesforceQueryUtil.class);
    PowerMockito.when(SalesforceQueryUtil.createCountQuery(query))
        .thenReturn("SELECT COUNT() FROM Opportunity");
    PowerMockito.when(SalesforceQueryUtil.getQueryPlan(Mockito.anyString(), Mockito.any()))
        .thenThrow(new RuntimeException("Query plan API error"));

    boolean result = SalesforceSplitUtil.hasRequiredCountForPkChunking(query, credentials, threshold);

    Assert.assertTrue(result);
  }

  @Test
  public void hasRequiredCountForPkChunking_countQueryFails_defaultsToTrue() throws Exception {
    String query = "SELECT Id, Name FROM Opportunity";
    AuthenticatorCredentials credentials = Mockito.mock(AuthenticatorCredentials.class);
    long threshold = 100000;
    PowerMockito.mockStatic(SalesforceQueryUtil.class);
    PowerMockito.when(SalesforceQueryUtil.createCountQuery(query))
        .thenReturn("SELECT COUNT() FROM Opportunity");
    SalesforceQueryUtil.QueryPlanResponse planResponse = Mockito.mock(SalesforceQueryUtil.QueryPlanResponse.class);
    SalesforceQueryUtil.QueryPlan plan = Mockito.mock(SalesforceQueryUtil.QueryPlan.class);
    Mockito.when(plan.getCardinality()).thenReturn(10L);
    Mockito.when(plan.getRelativeCost()).thenReturn(0.1);
    Mockito.when(planResponse.getPlans()).thenReturn(Collections.singletonList(plan));
    PowerMockito.when(SalesforceQueryUtil.getQueryPlan(Mockito.anyString(), Mockito.any()))
        .thenReturn(planResponse);
    PowerMockito.mockStatic(SalesforceConnectionUtil.class);
    PowerMockito.when(SalesforceConnectionUtil.getPartnerConnection(credentials))
        .thenThrow(new RuntimeException("Partner connection count query error"));

    boolean result = SalesforceSplitUtil.hasRequiredCountForPkChunking(query, credentials, threshold);

    Assert.assertTrue(result);
  }
}

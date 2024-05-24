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
import org.junit.Assert;
import org.junit.Test;

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
}

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

package co.cask.hydrator.salesforce;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link SalesforceQueryUtil}.
 */
public class SalesforceQueryUtilTest {

  @Test
  public void testCreateSObjectQueryWithoutFilter() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";
    int duration = 0;
    int offset = 0;
    String datetimeFilter = null;

    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName, duration, offset, datetimeFilter);

    Assert.assertNotNull(sObjectQuery);
    Assert.assertEquals("SELECT Id,Name,SomeField FROM sObjectName", sObjectQuery);
  }

  @Test
  public void testCreateSObjectQueryWithBlankDatetimeFilter() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";
    int duration = 0;
    int offset = 0;
    String datetimeFilter = "        ";

    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName, duration, offset, datetimeFilter);

    Assert.assertNotNull(sObjectQuery);
    Assert.assertEquals("SELECT Id,Name,SomeField FROM sObjectName", sObjectQuery);
  }

  @Test
  public void testCreateSObjectQueryWithDateLiteral() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";
    int duration = 0;
    int offset = 0;
    String datetimeFilter = "YESTERDAY";

    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName, duration, offset, datetimeFilter);

    Assert.assertNotNull(sObjectQuery);
    Assert.assertEquals("SELECT Id,Name,SomeField FROM sObjectName WHERE LastModifiedDate>YESTERDAY",
                        sObjectQuery);
  }

  @Test
  public void testCreateSObjectQueryWithDateLiteralFilterSkipSqlInjection() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";
    int duration = 0;
    int offset = 0;
    String datetimeFilter = "YESTERDAY OR Id LIKE '%'";

    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName, duration, offset, datetimeFilter);

    Assert.assertNotNull(sObjectQuery);
    Assert.assertEquals("SELECT Id,Name,SomeField FROM sObjectName WHERE LastModifiedDate>YESTERDAY",
                        sObjectQuery);
  }

  @Test
  public void testCreateSObjectQueryWithDuration() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";
    ZonedDateTime dateTime = ZonedDateTime.now(ZoneOffset.UTC);
    int duration = 6;
    int offset = 0;
    String datetimeFilter = null;
    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName, dateTime, duration, offset,
                                                                 datetimeFilter);

    Assert.assertNotNull(sObjectQuery);
    String expected = String.format("SELECT Id,Name,SomeField "
                                      + "FROM sObjectName "
                                      + "WHERE "
                                      + "LastModifiedDate>%s AND LastModifiedDate<%s",
                                    dateTime.minusHours(duration).format(DateTimeFormatter.ISO_DATE_TIME),
                                    dateTime.format(DateTimeFormatter.ISO_DATE_TIME));
    Assert.assertEquals(expected, sObjectQuery);
  }

  @Test
  public void testCreateSObjectQueryWithDurationAndOffset() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";
    ZonedDateTime dateTime = ZonedDateTime.now(ZoneOffset.UTC);
    int duration = 6;
    int offset = 1;
    String datetimeFilter = null;
    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName, dateTime, duration, offset,
                                                                 datetimeFilter);

    Assert.assertNotNull(sObjectQuery);
    String fromDate = dateTime.minusHours(duration).minusHours(offset).format(DateTimeFormatter.ISO_DATE_TIME);
    String toDate = dateTime.minusHours(offset).format(DateTimeFormatter.ISO_DATE_TIME);
    String expected = String.format("SELECT Id,Name,SomeField "
                                      + "FROM sObjectName "
                                      + "WHERE "
                                      + "LastModifiedDate>%s AND LastModifiedDate<%s",
                                    fromDate, toDate);

    Assert.assertEquals(expected, sObjectQuery);
  }

  @Test
  public void testCreateSObjectQueryWithAllFiltersProvided() {
    List<String> fields = Arrays.asList("Id", "Name", "SomeField");
    String sObjectName = "sObjectName";
    int duration = 10;
    int offset = 5;
    String datetimeFilter = "LAST_WEEK";

    String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(fields, sObjectName, duration, offset, datetimeFilter);

    Assert.assertNotNull(sObjectQuery);
    Assert.assertEquals("SELECT Id,Name,SomeField FROM sObjectName WHERE LastModifiedDate>LAST_WEEK",
                        sObjectQuery);
  }
}

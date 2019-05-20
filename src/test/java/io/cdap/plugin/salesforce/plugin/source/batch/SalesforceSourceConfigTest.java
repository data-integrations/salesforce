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

package io.cdap.plugin.salesforce.plugin.source.batch;

import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Tests for {@link SalesforceSourceConfig}.
 */
public class SalesforceSourceConfigTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetQuerySOQL() {
    String soql = "select id from sObject";
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setQuery(soql)
      .build();

    String query = config.getQuery(System.currentTimeMillis());

    Assert.assertNotNull(query);
    Assert.assertEquals(soql, query);
  }

  @Test
  public void testEmptyDuration() {
    Stream.of(
      null,
      "",
      "      ")
      .map(value -> new SalesforceSourceConfigBuilder().setDuration(value).build())
      .map(SalesforceBaseSourceConfig::getDuration)
      .forEach(duration -> Assert.assertEquals(Collections.emptyMap(), duration));
  }

  @Test
  public void testEmptyOffset() {
    Stream.of(
      null,
      "",
      "      ")
      .map(value -> new SalesforceSourceConfigBuilder().setOffset(value).build())
      .map(SalesforceBaseSourceConfig::getOffset)
      .forEach(offset -> Assert.assertEquals(Collections.emptyMap(), offset));
  }

  @Test
  public void testIsSoqlQueryTrue() {
    String soql = "select id from sObject";
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setQuery(soql)
      .setSObjectName(null)
      .build();

    Assert.assertTrue(config.isSoqlQuery());
  }

  @Test
  public void testIsSoqlQueryFalse() {
    String sObjectName = "SObject";
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setQuery(null)
      .setSObjectName(sObjectName)
      .build();

    Assert.assertFalse(config.isSoqlQuery());
  }

  @Test
  public void testIsSoqlQueryException() {
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setQuery(null)
      .setSObjectName(null)
      .build();

    thrown.expect(InvalidConfigPropertyException.class);

    config.isSoqlQuery();
  }

  @Test
  public void testGetDuration() {
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setDuration(" 2 YEARS,4 MONTHS,1 DAYS,2 HOURS,30 MINUTES,40  SECONDS")
      .build();

    Map<ChronoUnit, Integer> duration = config.getDuration();
    Assert.assertEquals(6, duration.size());
    Assert.assertEquals(new Integer(2), duration.get(ChronoUnit.YEARS));
    Assert.assertEquals(new Integer(4), duration.get(ChronoUnit.MONTHS));
    Assert.assertEquals(new Integer(1), duration.get(ChronoUnit.DAYS));
    Assert.assertEquals(new Integer(2), duration.get(ChronoUnit.HOURS));
    Assert.assertEquals(new Integer(30), duration.get(ChronoUnit.MINUTES));
    Assert.assertEquals(new Integer(40), duration.get(ChronoUnit.SECONDS));
  }

  @Test
  public void testGetDurationException() {
    Stream.of(
      "2 abc",
      "HOURS HOURS",
      "1;HOURS",
      "1    HOURS;2 DAYS",
      "1 HOURS,2",
      "1 days, 2 days, 1 hours"
    )
      .forEach(value -> {
        try {
          SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
            .setDuration(value)
            .build();

          config.getDuration();

          Assert.fail(String.format("Exception is not thrown for value '%s'", value));
        } catch (InvalidConfigPropertyException e) {
          // expected failure, do nothing
        }
      });
  }

  @Test
  public void testGetOffset() {
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setOffset("2   YEARS, 4 MONTHS, 1 DAYS, 2 HOURS, 30 MINUTES, 40 SECONDS ")
      .build();

    Map<ChronoUnit, Integer> offset = config.getOffset();
    Assert.assertEquals(6, offset.size());
    Assert.assertEquals(new Integer(2), offset.get(ChronoUnit.YEARS));
    Assert.assertEquals(new Integer(4), offset.get(ChronoUnit.MONTHS));
    Assert.assertEquals(new Integer(1), offset.get(ChronoUnit.DAYS));
    Assert.assertEquals(new Integer(2), offset.get(ChronoUnit.HOURS));
    Assert.assertEquals(new Integer(30), offset.get(ChronoUnit.MINUTES));
    Assert.assertEquals(new Integer(40), offset.get(ChronoUnit.SECONDS));
  }

  @Test
  public void testGetOffsetException() {
    Stream.of(
      "2 abc",
      "HOURS HOURS",
      "1;HOURS",
      "1 HOURS;2 DAYS",
      "1 HOURS,2",
      "1 days, 2 days, 1 hours"
    )
      .forEach(value -> {
        try {
          SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
            .setOffset(value)
            .build();

          config.getOffset();

          Assert.fail(String.format("Exception is not thrown for value '%s'", value));
        } catch (InvalidConfigPropertyException e) {
          // expected failure, do nothing
        }
      });
  }
}

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

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.salesforce.InvalidConfigException;
import io.cdap.plugin.salesforce.plugin.SalesforceConnectorConfig;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Tests for {@link SalesforceSourceConfig}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(SalesforceConnectorConfig.class)
public class SalesforceSourceConfigTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetQuerySOQL() {
    String soql = "select id from sObject";
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setQuery(soql)
      .build();

    String query = config.getQuery(System.currentTimeMillis(), null);

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

    thrown.expect(InvalidConfigException.class);

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
        } catch (InvalidConfigException e) {
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
        } catch (InvalidConfigException e) {
          // expected failure, do nothing
        }
      });
  }

  @Test
  public void testValidPKChunkConfig() {
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setQuery("Select Name from Table")
      .setEnablePKChunk(true)
      .setReferenceName("Source").build();
    testValidPKChunkConfiguration(config.getConnection());
  }

  @Test
  public void testValidPkChunkConfigWithMaxChunkSize() {
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setQuery("Select Name from Table")
      .setEnablePKChunk(true)
      .setChunkSize(SalesforceSourceConstants.MAX_PK_CHUNK_SIZE)
      .setReferenceName("Source").build();
    testValidPKChunkConfiguration(config.getConnection());
  }

  @Test
  public void testValidPkChunkConfigWithMinChunkSize() {
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setQuery("Select Name from Table")
      .setEnablePKChunk(true)
      .setChunkSize(SalesforceSourceConstants.MIN_PK_CHUNK_SIZE)
      .setReferenceName("Source").build();
    testValidPKChunkConfiguration(config.getConnection());
  }

  private void testValidPKChunkConfiguration(SalesforceConnectorConfig config) {
    MockFailureCollector collector = new MockFailureCollector();
    SalesforceConnectorConfig mock = Mockito.spy(config);
    Mockito.when(mock.canAttemptToEstablishConnection()).thenReturn(false);
    mock.validate(collector, null);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testPKChunkWithRestrictedQuery() throws Exception {
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setReferenceName("Source")
      .setQuery("Select Max(Id) MX from Table")
      .setEnablePKChunk(true)
      .build();
    testPKChunkInvalidConfig(config, SalesforceSourceConstants.PROPERTY_QUERY);
  }

  @Test
  public void testPKChunkWithChunkSizeAboveMax() throws Exception {
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setQuery("Select Name from Table")
      .setEnablePKChunk(true)
      .setChunkSize(SalesforceSourceConstants.MAX_PK_CHUNK_SIZE + 1)
      .setReferenceName("Source").build();
    testPKChunkInvalidConfig(config, SalesforceSourceConstants.PROPERTY_CHUNK_SIZE_NAME);
  }

  @Test
  public void testPKChunkWithChunkSizeBelowMin() throws Exception {
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setQuery("Select Name from Table")
      .setEnablePKChunk(true)
      .setChunkSize(SalesforceSourceConstants.MIN_PK_CHUNK_SIZE - 1)
      .setReferenceName("Source").build();
    testPKChunkInvalidConfig(config, SalesforceSourceConstants.PROPERTY_CHUNK_SIZE_NAME);
  }

  private void testPKChunkInvalidConfig(SalesforceSourceConfig config, String stageConfigName) throws Exception {
    MockFailureCollector collector = new MockFailureCollector();
    SalesforceSourceConfig mock = Mockito.spy(config);
    SalesforceConnectorConfig connectorConfig = Mockito.mock(SalesforceConnectorConfig.class);
    PowerMockito.whenNew(SalesforceConnectorConfig.class).withArguments(Mockito.anyString(), Mockito.anyString(),
                                                                        Mockito.anyString(), Mockito.anyString(),
                                                                        Mockito.anyString(), Mockito.anyString(),
                                                                        Mockito.any(),
                                                                        Mockito.any()).thenReturn(connectorConfig);
    Mockito.when(mock.getConnection()).thenReturn(connectorConfig);
    PowerMockito.when(connectorConfig.canAttemptToEstablishConnection()).thenReturn(false);
    ValidationFailure failure;
    try {
      mock.validate(collector, null);
      Assert.assertEquals(1, collector.getValidationFailures().size());
      failure = collector.getValidationFailures().get(0);
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      failure = e.getFailures().get(0);
    }
    Assert.assertEquals(stageConfigName, failure.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }
}

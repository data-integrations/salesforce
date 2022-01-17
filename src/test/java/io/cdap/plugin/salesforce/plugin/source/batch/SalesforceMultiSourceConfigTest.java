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
package io.cdap.plugin.salesforce.plugin.source.batch;

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link SalesforceMultiSourceConfig}.
 */
public class SalesforceMultiSourceConfigTest {

  @Test
  public void testGetWhiteList() {
    String whiteList = "Account,Contact";
    SalesforceMultiSourceConfig config = new SalesforceMultiSourceConfigBuilder()
      .setWhiteList(whiteList)
      .build();

    Set<String> sObjects = config.getWhiteList();
    Assert.assertNotNull(sObjects);
    Assert.assertEquals(whiteList, String.join(",", sObjects));

  }

  @Test
  public void testGetBlackList() {
    String blackList = "Lead,Contact";
    SalesforceMultiSourceConfig config = new SalesforceMultiSourceConfigBuilder()
      .setBlackList(blackList)
      .build();

    Set<String> sObjects = config.getBlackList();
    Assert.assertNotNull(sObjects);
    Assert.assertEquals(blackList, String.join(",", sObjects));

  }

  @Test
  public void testGetSObjectNameField() {
    String sObjectFieldName = "Account";
    SalesforceMultiSourceConfig config = new SalesforceMultiSourceConfigBuilder()
      .setsObjectNameField(sObjectFieldName)
      .build();

    String sObject = config.getSObjectNameField();
    Assert.assertNotNull(sObject);
    Assert.assertEquals(sObjectFieldName, sObject);
  }

  @Test
  public void testValidate() {
    SalesforceMultiSourceConfig config = new SalesforceMultiSourceConfigBuilder().setsObjectNameField("Account")
      .setDuration("InvalidDuration").
      build();
    MockFailureCollector collector = new MockFailureCollector();
    SalesforceMultiSourceConfig mock = Mockito.spy(config);
    Mockito.when(mock.canAttemptToEstablishConnection()).thenReturn(false);
    mock.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(
      "'duration' has invalid format 'InvalidDuration'. " +
        "Expected format is <VALUE_1> <TYPE_1>,<VALUE_2> <TYPE_2>... . For example, '1 days, 2 hours, 30 minutes'",
      collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testGetQueries() {
    SalesforceMultiSourceConfig mock = Mockito.mock(SalesforceMultiSourceConfig.class);
    Mockito.when(mock.getQueries(Mockito.anyLong())).thenReturn(Arrays.asList("Account", "Contact"));
    List<String> sObjects = mock.getQueries(50000);
    Assert.assertEquals(sObjects.size(), 2);
  }

  @Test
  public void testGetOffset() {
    SalesforceMultiSourceConfig config = new SalesforceMultiSourceConfigBuilder()
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
  public void testGetDuration() {
    SalesforceMultiSourceConfig config = new SalesforceMultiSourceConfigBuilder()
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


}

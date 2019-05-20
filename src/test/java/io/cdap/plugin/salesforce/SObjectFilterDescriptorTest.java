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

import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.stream.Stream;

/**
 * Tests for {@link SObjectFilterDescriptor}.
 */
public class SObjectFilterDescriptorTest {

  @Test
  public void testIsNoOp() {
    Stream.of(
      SObjectFilterDescriptor.noOp(),
      SObjectFilterDescriptor.interval(null, null),
      SObjectFilterDescriptor.range(System.currentTimeMillis(), Collections.emptyMap(), Collections.emptyMap()),
      SObjectFilterDescriptor.range(System.currentTimeMillis(),
                                    Collections.singletonMap(ChronoUnit.DAYS, 0),
                                    Collections.singletonMap(ChronoUnit.HOURS, 0))
      )
      .forEach(filter -> Assert.assertTrue(String.format("Filter must be no-op '%s'", filter), filter.isNoOp()));
  }

  @Test
  public void testRangeDuration() {
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    SObjectFilterDescriptor filter = SObjectFilterDescriptor.range(
      now.toInstant().toEpochMilli(),
      Collections.singletonMap(ChronoUnit.HOURS, 6),
      Collections.emptyMap());

    Assert.assertEquals(now.minusHours(6), filter.getStartTime());
    Assert.assertEquals(now, filter.getEndTime());
  }

  @Test
  public void testRangeOffset() {
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    SObjectFilterDescriptor filter = SObjectFilterDescriptor.range(
      now.toInstant().toEpochMilli(),
      Collections.emptyMap(),
      Collections.singletonMap(ChronoUnit.HOURS, 2));

    Assert.assertNull(filter.getStartTime());
    Assert.assertEquals(now.minusHours(2), filter.getEndTime());
  }

  @Test
  public void testRangeDurationAndOffset() {
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    SObjectFilterDescriptor filter = SObjectFilterDescriptor.range(
      now.toInstant().toEpochMilli(),
      Collections.singletonMap(ChronoUnit.HOURS, 6),
      Collections.singletonMap(ChronoUnit.HOURS, 2));

    Assert.assertEquals(now.minusHours(2).minusHours(6), filter.getStartTime());
    Assert.assertEquals(now.minusHours(2), filter.getEndTime());
  }
}

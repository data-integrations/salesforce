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

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * SObject query datetime filter handler. Can be of three types:
 * <ul>
 * <li>Interval - filter using provided start and end datetime</li>
 * <li>Range - filter calculated from provided start time, duration and offset</li>
 * <li>NoOp - filter initialized with null</li>
 * </ul>
 */
public final class SObjectFilterDescriptor {

  private static final SObjectFilterDescriptor NO_OP_FILTER_INSTANCE = new SObjectFilterDescriptor(null, null);

  @Nullable
  private final ZonedDateTime startTime;
  @Nullable
  private final ZonedDateTime endTime;

  public static SObjectFilterDescriptor noOp() {
    return NO_OP_FILTER_INSTANCE;
  }

  public static SObjectFilterDescriptor interval(@Nullable ZonedDateTime startTime,
                                                 @Nullable ZonedDateTime endTime) {
    return startTime == null && endTime == null
      ? NO_OP_FILTER_INSTANCE
      : new SObjectFilterDescriptor(startTime, endTime);
  }

  public static SObjectFilterDescriptor range(long logicalStartTime,
                                              Map<ChronoUnit, Integer> duration,
                                              Map<ChronoUnit, Integer> offset) {
    return calculateRangeFilter(toZonedDateTime(logicalStartTime), duration, offset);
  }

  private SObjectFilterDescriptor(@Nullable ZonedDateTime startTime,
                                  @Nullable ZonedDateTime endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
  }

  @Nullable
  public ZonedDateTime getStartTime() {
    return startTime;
  }

  @Nullable
  public ZonedDateTime getEndTime() {
    return endTime;
  }

  public boolean isNoOp() {
    return equals(NO_OP_FILTER_INSTANCE);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startTime, endTime);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SObjectFilterDescriptor that = (SObjectFilterDescriptor) o;
    return Objects.equals(startTime, that.startTime) &&
      Objects.equals(endTime, that.endTime);
  }

  @Override
  public String toString() {
    return "SObjectFilterDescriptor{" +
      "startTime=" + startTime +
      ", endTime=" + endTime +
      '}';
  }

  /**
   * Generates Range or NoOp SObject query filter using provided values.
   *
   * @param logicalStartTime application start time
   * @param duration         filter duration
   * @param offset           filter offset
   * @return instance of SObjectFilterDescriptor
   */
  private static SObjectFilterDescriptor calculateRangeFilter(ZonedDateTime logicalStartTime,
                                                              Map<ChronoUnit, Integer> duration,
                                                              Map<ChronoUnit, Integer> offset) {
    if (duration.isEmpty() && offset.isEmpty()) {
      // no filter is required
      return NO_OP_FILTER_INSTANCE;
    }

    ZonedDateTime endTime = logicalStartTime;
    for (Map.Entry<ChronoUnit, Integer> entry : offset.entrySet()) {
      endTime = endTime.minus(entry.getValue(), entry.getKey());
    }
    ZonedDateTime startTime = endTime;
    for (Map.Entry<ChronoUnit, Integer> entry : duration.entrySet()) {
      startTime = startTime.minus(entry.getValue(), entry.getKey());
    }
    // if duration and offset are zero (Example: 0 Hours)
    if (startTime.equals(endTime) && endTime.equals(logicalStartTime)) {
      // no filter is required
      return NO_OP_FILTER_INSTANCE;
    }
    return new SObjectFilterDescriptor(startTime.equals(endTime) ? null : startTime, endTime);
  }

  /**
   * Transforms provided time from millis to {@link ZonedDateTime}.
   *
   * @param timeInMillis logical start time
   * @return datetime in UTC timezone
   */
  private static ZonedDateTime toZonedDateTime(long timeInMillis) {
    return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timeInMillis), ZoneOffset.UTC);
  }
}

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
package io.cdap.plugin.salesforce.plugin.sink.batch;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Represents a CSV record with its headers.
 */
public class CSVRecord implements Iterable<String> {
  private final List<String> columnNames;
  private final List<String> values;

  public CSVRecord(List<String> columnNames, List<String> values) {
    this.columnNames = columnNames;
    this.values = values;

    Preconditions.checkState(columnNames.size() == values.size(),
                             String.format("Cannot create CSVRecord, with " +
                                             "number of columns %d not equal to number of values %d",
                                           columnNames.size(), values.size()));
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<String> getValues() {
    return values;
  }

  @Override
  public Iterator<String> iterator() {
    return values.iterator();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CSVRecord that = (CSVRecord) o;

    return (Objects.equals(columnNames, that.columnNames) &&
      Objects.equals(values, that.values));
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnNames, values);
  }

  @Override
  public String toString() {
    return "CSVRecord{" +
      "columnNames=" + Arrays.toString(columnNames.toArray()) +
      ", values=" + Arrays.toString(values.toArray()) +
      '}';
  }
}

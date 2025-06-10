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

import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;
import org.mockito.internal.util.reflection.FieldSetter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SalesforceWideRecordReaderTest {
  @Test
  public void testWideRecordReader_withOutputRecords() throws Exception {
    Schema schema = Schema.recordOf("test", Schema.Field.of("Id", Schema.of(Schema.Type.STRING)));
    SoapRecordToMapTransformer transformer = mock(SoapRecordToMapTransformer.class);
    SalesforceWideRecordReader reader = new SalesforceWideRecordReader(schema, "SELECT Id FROM Account", transformer);

    Map<String, Object> mockRecord = new HashMap<>();
    mockRecord.put("Id", "id_123");

    List<Map<String, ?>> results = new ArrayList<>();
    results.add(mockRecord);

    FieldSetter.setField(reader, SalesforceWideRecordReader.class.getDeclaredField("results"), results);
    FieldSetter.setField(reader, SalesforceWideRecordReader.class.getDeclaredField("batchIterator"),
      Collections.<List<Map<String, ?>>>emptyIterator());

    assertEquals(0.0f, reader.getProgress(), 0.0001);

    assert reader.nextKeyValue();
    Map<String, ?> value = reader.getCurrentValue();
    assertEquals("id_123", value.get("Id"));
    assert value.containsKey("Id");
    assertEquals(1.0f, reader.getProgress(), 0.0001);
    assert !reader.nextKeyValue();
    assertEquals("id_123", reader.getCurrentValue().get("Id"));
  }

  @Test
  public void testWideRecordReader_fetchBatchRecordsCalledExpectedNumberOfTimes() throws Exception {
    Schema schema = Schema.recordOf("test", Schema.Field.of("Id", Schema.of(Schema.Type.STRING)));
    SoapRecordToMapTransformer transformer = mock(SoapRecordToMapTransformer.class);
    SalesforceWideRecordReader reader = spy(new SalesforceWideRecordReader(
      schema, "SELECT Id FROM Account", transformer));

    doNothing().when(reader).initialize(any(InputSplit.class), any(TaskAttemptContext.class));

    List<List<Map<String, ?>>> mockPartitions = Arrays.asList(
      Collections.singletonList(Collections.singletonMap("Id", "id_1")),
      Collections.singletonList(Collections.singletonMap("Id", "id_2")),
      Collections.singletonList(Collections.singletonMap("Id", "id_3"))
    );

    Iterator<List<Map<String, ?>>> batchIterator = mockPartitions.iterator();
    FieldSetter.setField(reader, SalesforceWideRecordReader.class.getDeclaredField("batchIterator"), batchIterator);

    AtomicInteger batchIndex = new AtomicInteger(0);
    doAnswer(invocation -> {
      int index = batchIndex.getAndIncrement();
      if (index < mockPartitions.size()) {
        return mockPartitions.get(index);
      } else {
        return Collections.emptyList();
      }
    }).when(reader).fetchBatchRecords();

    FieldSetter.setField(reader, SalesforceWideRecordReader.class.getDeclaredField("results"), new ArrayList<>());
    FieldSetter.setField(reader, SalesforceWideRecordReader.class.getDeclaredField("index"), 0);

    List<Map<String, ?>> readRecords = new ArrayList<>();
    while (reader.nextKeyValue()) {
      readRecords.add(reader.getCurrentValue());
    }

    verify(reader, times(4)).fetchBatchRecords();
    assertEquals(3, readRecords.size());
  }
}

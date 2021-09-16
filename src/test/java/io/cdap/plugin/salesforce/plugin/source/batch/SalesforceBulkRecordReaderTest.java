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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sforce.async.BulkConnection;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SalesforceBulkRecordReaderTest {
  @Test
  public void testMultipleResults() throws Exception {
    String csvString1 = "\"Id\",\"IsDeleted\",\"ExpectedRevenue\",\"LastModifiedDate\",\"CloseDate\",\"Time\"\n" +
      "\"0061i000003XNcBAAW\",\"false\",\"1500.0\",\"2019-02-22T07:03:21.000Z\",\"2019-01-01\",\"12:00:30.000Z\"\n";
    String csvString2 = "\"Id\",\"IsDeleted\",\"ExpectedRevenue\",\"LastModifiedDate\",\"CloseDate\",\"Time\"\n" +
      "\"0061i000003XNcCAAW\",\"false\",\"112500.0\",\"2019-02-22T07:03:21.000Z\",\"2018-12-20\",\"12:00:40.000Z\"\n" +
        "\"0061i000003XNcDAAW\",\"false\",\"220000.0\",\"2019-02-22T07:03:21.000Z\",\"2018-11-15\",\"12:00:50.000Z\"\n";

    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("Id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("IsDeleted", Schema.of(Schema.Type.BOOLEAN)),
                                    Schema.Field.of("ExpectedRevenue", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("LastModifiedDate", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("CloseDate", Schema.of(Schema.LogicalType.DATE)),
                                    Schema.Field.of("Time", Schema.of(Schema.LogicalType.TIME_MICROS))
    );

    List<Map<String, Object>> expectedRecords = new ImmutableList.Builder<Map<String, Object>>()
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcBAAW")
             .put("IsDeleted", false)
             .put("ExpectedRevenue", 1500.0)
             .put("LastModifiedDate", 1550819001000000L)
             .put("CloseDate", 17897)
             .put("Time", 43230000000L)
             .build()
      )
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcCAAW")
             .put("IsDeleted", false)
             .put("ExpectedRevenue", 112500.0)
             .put("LastModifiedDate", 1550819001000000L)
             .put("CloseDate", 17885)
             .put("Time", 43240000000L)
             .build()
      )
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcDAAW")
             .put("IsDeleted", false)
             .put("ExpectedRevenue", 220000.0)
             .put("LastModifiedDate", 1550819001000000L)
             .put("Time", 43250000000L)
             .put("CloseDate", 17850)
             .build()
      )
      .build();

    assertRecordReaderOutputRecords(new String[] {csvString1, csvString2}, schema, expectedRecords);
  }

  @Test
  public void testEmptyResult() throws Exception {
    String csvString1 = "\"Id\",\"IsDeleted\",\"ExpectedRevenue\",\"LastModifiedDate\",\"CloseDate\",\"Time\"\n" +
      "\"0061i000003XNcBAAW\",\"false\",\"1500.0\",\"2019-02-22T07:03:21.000Z\",\"2019-01-01\",\"12:00:30.000Z\"\n" +
      "\"0061i000003XNcCAAW\",\"false\",\"112500.0\",\"2019-02-22T07:03:21.000Z\",\"2018-12-20\",\"12:00:40.000Z\"\n";
      String csvString2 = "\"Id\",\"IsDeleted\",\"ExpectedRevenue\",\"LastModifiedDate\",\"CloseDate\",\"Time\"\n" +
      "\"0061i000003XNcDAAW\",\"false\",\"220000.0\",\"2019-02-22T07:03:21.000Z\",\"2018-11-15\",\"12:00:50.000Z\"\n";
    String csvString3 = "\"Id\",\"IsDeleted\",\"ExpectedRevenue\",\"LastModifiedDate\",\"CloseDate\",\"Time\"\n";


    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("Id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("IsDeleted", Schema.of(Schema.Type.BOOLEAN)),
                                    Schema.Field.of("ExpectedRevenue", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("LastModifiedDate", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("CloseDate", Schema.of(Schema.LogicalType.DATE)),
                                    Schema.Field.of("Time", Schema.of(Schema.LogicalType.TIME_MICROS))
    );

    List<Map<String, Object>> expectedRecords = new ImmutableList.Builder<Map<String, Object>>()
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcBAAW")
             .put("IsDeleted", false)
             .put("ExpectedRevenue", 1500.0)
             .put("LastModifiedDate", 1550819001000000L)
             .put("CloseDate", 17897)
             .put("Time", 43230000000L)
             .build()
      )
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcCAAW")
             .put("IsDeleted", false)
             .put("ExpectedRevenue", 112500.0)
             .put("LastModifiedDate", 1550819001000000L)
             .put("CloseDate", 17885)
             .put("Time", 43240000000L)
             .build()
      )
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcDAAW")
             .put("IsDeleted", false)
             .put("ExpectedRevenue", 220000.0)
             .put("LastModifiedDate", 1550819001000000L)
             .put("Time", 43250000000L)
             .put("CloseDate", 17850)
             .build()
      )
      .build();

    assertRecordReaderOutputRecords(new String[] {csvString1, csvString2, csvString3}, schema, expectedRecords);
  }

  @Test
  public void testUTF8InKeysAndValues() throws Exception {
    String csvString = "\"Id\",\"IsDeleted\u0628\u0633\u0645\",\"ExpectedRevenue\"," +
      "\"LastModifiedDate\",\"CloseDate\"\n" +
      "\"0061i000003XNcBAAW\u0628\u0633\u0645\",\"false\",\"1500.0\",\"2019-02-22T07:03:21.000Z\",\"2019-01-01\"\n" +
      "\"0061i000003XNcCAAW\",\"false\",\"112500.0\",\"2019-02-22T07:03:21.000Z\",\"2018-12-20\"\n" +
      "\"0061i000003XNcDAAW\",\"false\",\"220000.0\",\"2019-02-22T07:03:21.000Z\",\"2018-11-15\"";

    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("Id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field
                                      .of(SalesforceSchemaUtil.normalizeAvroName("IsDeleted\u0628\u0633\u0645"),
                                          Schema.of(Schema.Type.BOOLEAN)),
                                    Schema.Field.of("ExpectedRevenue", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("LastModifiedDate", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("CloseDate", Schema.of(Schema.LogicalType.DATE))
    );

    List<Map<String, Object>> expectedRecords = new ImmutableList.Builder<Map<String, Object>>()
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcBAAW\u0628\u0633\u0645")
             .put(SalesforceSchemaUtil.normalizeAvroName("IsDeleted\u0628\u0633\u0645"), false)
             .put("ExpectedRevenue", 1500.0)
             .put("LastModifiedDate", 1550819001000000L)
             .put("CloseDate", 17897)
             .build()
      )
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcCAAW")
             .put(SalesforceSchemaUtil.normalizeAvroName("IsDeleted\u0628\u0633\u0645"), false)
             .put("ExpectedRevenue", 112500.0)
             .put("LastModifiedDate", 1550819001000000L)
             .put("CloseDate", 17885)
             .build()
      )
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcDAAW")
             .put(SalesforceSchemaUtil.normalizeAvroName("IsDeleted\u0628\u0633\u0645"), false)
             .put("ExpectedRevenue", 220000.0)
             .put("LastModifiedDate", 1550819001000000L)
             .put("CloseDate", 17850)
             .build()
      )
      .build();

    assertRecordReaderOutputRecords(new String[] {csvString}, schema, expectedRecords);
  }

  @Test
  public void testInvalidCSV() throws Exception {
    // this csv is invalid since values are not quoted
    String csvString = "key1,\"\"key2,key3\n" +
      "value1,value2,value3";

    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("key1", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("key2", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("key3", Schema.of(Schema.Type.STRING))
    );

    List<Map<String, Object>> expectedRecords = new ImmutableList.Builder<Map<String, Object>>().build();

    try {
      assertRecordReaderOutputRecords(new String[] {csvString}, schema, expectedRecords);
      Assert.fail("Expected to throw exception due to not different number of arguments");
    } catch (IOException ex) {
      Assert.assertTrue(ex.getMessage().contains("invalid char between encapsulated token and delimiter"));
    }
  }

  @Test
  public void testEmptyCSVResponse() throws Exception {
    // CSV without headers is not valid
    String csvString = "";

    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("key1", Schema.of(Schema.Type.STRING))
    );

    List<Map<String, Object>> expectedRecords = new ImmutableList.Builder<Map<String, Object>>().build();

    try {
      assertRecordReaderOutputRecords(new String[] {csvString}, schema, expectedRecords);
      Assert.fail("Expected to throw exception due to not different number of arguments");
    } catch (IllegalStateException ex) {
      Assert.assertTrue(ex.getMessage().
        contains("Empty response was received from Salesforce, but csv header was expected"));
    }
  }

  @Test
  public void testLineBreakAndCommaInCSV() throws Exception {
    String csvString = "\"Id\",\"ShippingStreet\"\n" +
      "\"0061i000003XNcBAAW\",\"1301 Hoch Drive\"\n" +
      "\"0061i000003XNcCAAW\",\"1301 Avenue of the Americas \n" +
      "New York, NY 10019\n" +
      "USA\"\n" +
      "\"0061i000003XNcDAAW\",\"620 SW 5th Avenue Suite 400\n" +
      "Portland, Oregon 97204\n" +
      "United States\"\n" +
      "\"0061i000003XNcEAAW\",\"345 Shoreline Park\n" +
      "Mountain View, CA 94043\n" +
      "USA\"";

    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("Id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("ShippingStreet", Schema.of(Schema.Type.STRING))
    );

    List<Map<String, Object>> expectedRecords = new ImmutableList.Builder<Map<String, Object>>()
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcBAAW")
             .put("ShippingStreet", "1301 Hoch Drive")
             .build()
      )
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcCAAW")
             .put("ShippingStreet", "1301 Avenue of the Americas \n" +
               "New York, NY 10019\n" +
               "USA")
             .build()
      )
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcDAAW")
             .put("ShippingStreet", "620 SW 5th Avenue Suite 400\n" +
               "Portland, Oregon 97204\n" +
               "United States")
             .build()
      )
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcEAAW")
             .put("ShippingStreet", "345 Shoreline Park\n" +
               "Mountain View, CA 94043\n" +
               "USA")
             .build()
      )
      .build();

    assertRecordReaderOutputRecords(new String[] {csvString}, schema, expectedRecords);
  }

  private void assertRecordReaderOutputRecords(String[] csvStrings, Schema schema,
                                               List<Map<String, Object>> expectedRecords) throws Exception {
    MapToRecordTransformer transformer = new MapToRecordTransformer();
    String jobId = "job";
    String batchId = "batch";
    String[] resultIds = new String[csvStrings.length];
    for (int i = 0; i < csvStrings.length; i++) {
      resultIds[i] = String.format("result%d", i);
    }

    SalesforceBulkRecordReader reader = new SalesforceBulkRecordReader(schema, jobId, batchId, resultIds);
    BulkConnection mock = Mockito.mock(BulkConnection.class);
    FieldSetter.setField(reader, SalesforceBulkRecordReader.class.getDeclaredField("bulkConnection"), mock);
    for (int i = 0; i < csvStrings.length; i++) {
      Mockito.when(mock.getQueryResultStream(jobId, batchId, resultIds[i]))
        .thenReturn(new ByteArrayInputStream(csvStrings[i].getBytes(StandardCharsets.UTF_8)));
    }
    reader.setupParser();

    Field fieldsField = StructuredRecord.class.getDeclaredField("fields");
    fieldsField.setAccessible(true);

    List<StructuredRecord> records = new ArrayList<>();
    while (reader.nextKeyValue()) {
      Map<String, ?> value = reader.getCurrentValue();
      StructuredRecord record = transformer.transform(schema, value);
      records.add(record);
    }

    for (StructuredRecord record : records) {
      Map<String, Object> fields = (Map<String, Object>) fieldsField.get(record);
      Assert.assertTrue(expectedRecords.contains(fields));
    }
  }
}

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

package co.cask.hydrator.salesforce.plugin.source.batch;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SalesforceRecordReaderTest {
  @Test
  public void testTypes() throws Exception {
    String csvString = "\"Id\",\"IsDeleted\",\"ExpectedRevenue\",\"LastModifiedDate\",\"CloseDate\"\n" +
      "\"0061i000003XNcBAAW\",\"false\",\"1500.0\",\"2019-02-22T07:03:21.000Z\",\"2019-01-01\"\n" +
      "\"0061i000003XNcCAAW\",\"false\",\"112500.0\",\"2019-02-22T07:03:21.000Z\",\"2018-12-20\"\n" +
      "\"0061i000003XNcDAAW\",\"false\",\"220000.0\",\"2019-02-22T07:03:21.000Z\",\"2018-11-15\"";

    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("Id", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("IsDeleted", Schema.of(Schema.Type.BOOLEAN)),
                                    Schema.Field.of("ExpectedRevenue", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("LastModifiedDate", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("CloseDate", Schema.of(Schema.LogicalType.DATE))
    );

    List<Map<String, Object>> expectedRecords = new ImmutableList.Builder<Map<String, Object>>()
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcBAAW")
             .put("IsDeleted", false)
             .put("ExpectedRevenue", 1500.0)
             .put("LastModifiedDate", 1550819001000L)
             .put("CloseDate", 17897)
             .build()
      )
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcCAAW")
             .put("IsDeleted", false)
             .put("ExpectedRevenue", 112500.0)
             .put("LastModifiedDate", 1550819001000L)
             .put("CloseDate", 17885)
             .build()
      )
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcDAAW")
             .put("IsDeleted", false)
             .put("ExpectedRevenue", 220000.0)
             .put("LastModifiedDate", 1550819001000L)
             .put("CloseDate", 17850)
             .build()
      )
      .build();

    assertRecordReaderOutputRecords(csvString, schema, expectedRecords);
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
                                    Schema.Field.of("IsDeleted\u0628\u0633\u0645", Schema.of(Schema.Type.BOOLEAN)),
                                    Schema.Field.of("ExpectedRevenue", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("LastModifiedDate", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("CloseDate", Schema.of(Schema.LogicalType.DATE))
    );

    List<Map<String, Object>> expectedRecords = new ImmutableList.Builder<Map<String, Object>>()
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcBAAW\u0628\u0633\u0645")
             .put("IsDeleted\u0628\u0633\u0645", false)
             .put("ExpectedRevenue", 1500.0)
             .put("LastModifiedDate", 1550819001000L)
             .put("CloseDate", 17897)
             .build()
      )
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcCAAW")
             .put("IsDeleted\u0628\u0633\u0645", false)
             .put("ExpectedRevenue", 112500.0)
             .put("LastModifiedDate", 1550819001000L)
             .put("CloseDate", 17885)
             .build()
      )
      .add(new ImmutableMap.Builder<String, Object>()
             .put("Id", "0061i000003XNcDAAW")
             .put("IsDeleted\u0628\u0633\u0645", false)
             .put("ExpectedRevenue", 220000.0)
             .put("LastModifiedDate", 1550819001000L)
             .put("CloseDate", 17850)
             .build()
      )
      .build();

    assertRecordReaderOutputRecords(csvString, schema, expectedRecords);
  }

  @Test
  public void testKeysAndValuesDifferentNumber() throws Exception {
    String csvString = "\"key1\",\"key2\",\"key3\"\n" +
      "\"value1\",\"value2\",\"value3\",\"value4\"";

    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("key1", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("key2", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("key3", Schema.of(Schema.Type.STRING))
    );

    List<Map<String, Object>> expectedRecords = new ImmutableList.Builder<Map<String, Object>>().build();

    try {
      assertRecordReaderOutputRecords(csvString, schema, expectedRecords);
      Assert.fail("Expected to throw exception due to not different number of arguments");
    } catch (IllegalArgumentException ex) {
      Assert.assertTrue(ex.getMessage().contains("is not consistent to a csv mapping"));
    }
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
      assertRecordReaderOutputRecords(csvString, schema, expectedRecords);
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
      assertRecordReaderOutputRecords(csvString, schema, expectedRecords);
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

    assertRecordReaderOutputRecords(csvString, schema, expectedRecords);
  }


  private void assertRecordReaderOutputRecords(String csvString, Schema schema,
                                               List<Map<String, Object>> expectedRecords)
    throws Exception {
    Emitter<StructuredRecord> emitter = mock(Emitter.class);

    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setReferenceName("myReferenceName")
      .setClientId("myClientId")
      .setClientSecret("myClientSecret")
      .setUsername("myUsername")
      .setPassword("myPassword")
      .setLoginUrl("myLoginUrl")
      .setErrorHandling("Stop on error")
      .setQuery("myQuery")
      .build();

    SalesforceBatchSource salesforceBatchSource = new SalesforceBatchSource(config);
    salesforceBatchSource.setSchema(schema);

    Field fieldsField = StructuredRecord.class.getDeclaredField("fields");
    fieldsField.setAccessible(true);

    SalesforceRecordReader rr = new SalesforceRecordReader();
    rr.setupParser(csvString);

    ArgumentCaptor<StructuredRecord> argument = ArgumentCaptor.forClass(StructuredRecord.class);

    while (rr.nextKeyValue()) {
      KeyValue<NullWritable, CSVRecord> keyValue = new KeyValue<>(null, rr.getCurrentValue());
      salesforceBatchSource.transform(keyValue, emitter);
    }

    verify(emitter, times(expectedRecords.size())).emit(argument.capture());
    List<StructuredRecord> records = argument.getAllValues();

    for (StructuredRecord record : records) {
      Map<String, Object> fields = (Map<String, Object>) fieldsField.get(record);
      Assert.assertTrue(expectedRecords.contains(fields));
    }
  }
}

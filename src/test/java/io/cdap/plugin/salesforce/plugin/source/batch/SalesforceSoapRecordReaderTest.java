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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SalesforceSoapRecordReaderTest {

  @Test
  public void testReader() {
    Schema schema = Schema.of(Schema.LogicalType.DATE);
    SalesforceSoapRecordReader actualSalesforceSoapRecordReader = new SalesforceSoapRecordReader(schema, "Query",
      new SoapRecordToMapTransformer());
    actualSalesforceSoapRecordReader.close();
    assertEquals(0.0f, actualSalesforceSoapRecordReader.getProgress(), 0.0f);
  }
}


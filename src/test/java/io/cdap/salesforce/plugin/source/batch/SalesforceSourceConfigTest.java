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

package io.cdap.salesforce.plugin.source.batch;

import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

    String query = config.getQuery();

    Assert.assertNotNull(query);
    Assert.assertEquals(soql, query);
  }

  @Test
  public void testGetDurationDefault() {
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setDuration(null)
      .build();

    Assert.assertEquals(0, config.getDuration());
  }

  @Test
  public void testGetOffsetDefault() {
    SalesforceSourceConfig config = new SalesforceSourceConfigBuilder()
      .setOffset(null)
      .build();

    Assert.assertEquals(0, config.getOffset());
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
}

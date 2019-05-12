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

import com.sforce.soap.partner.sobject.SObject;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.SalesforceFunctionType;
import io.cdap.plugin.salesforce.etl.SObjectBuilder;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SoapRecordToMapTransformerTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testTransformToMap() {
    SoapRecordToMapTransformer transformer = new SoapRecordToMapTransformer();
    SObject campaign = new SObjectBuilder()
      .setType("Campaign")
      .put("Id", "testCampaignId")
      .put("Name", "TestCampaign-1")
      .build();

    SObject opportunity = new SObjectBuilder()
      .setType("Opportunity")
      .put("Id", "testOpportunityId")
      .put("Name", "testOpportunity-1")
      .put("Amount", "25000")
      .put(campaign.getType(), campaign)
      .build();

    List<SObjectDescriptor.FieldDescriptor> fieldDescriptors =
      getFieldDescriptors("Name", "Id", "Campaign.Name", "Campaign.Id");
    SObjectDescriptor sObjectDescriptor = Mockito.mock(SObjectDescriptor.class);

    Mockito.when(sObjectDescriptor.getChildSObjects()).thenReturn(Collections.emptyList());
    Mockito.when(sObjectDescriptor.getFields()).thenReturn(fieldDescriptors);

    Map<String, ?> resultMap = transformer.transformToMap(opportunity, sObjectDescriptor);

    Assert.assertNotNull(resultMap);
    Assert.assertEquals(fieldDescriptors.size(), resultMap.size());

    Assert.assertEquals(opportunity.getField("Name"), resultMap.get("Name"));
    Assert.assertEquals(opportunity.getField("Id"), resultMap.get("Id"));
    Assert.assertEquals(campaign.getField("Name"), resultMap.get("Campaign.Name"));
    Assert.assertEquals(campaign.getField("Id"), resultMap.get("Campaign.Id"));
  }

  @Test
  public void testTransformToMapIncorrectReferenceField() {
    SoapRecordToMapTransformer transformer = new SoapRecordToMapTransformer();
    SObject opportunity = new SObjectBuilder()
      .setType("Opportunity")
      .put("Id", "testOpportunityId")
      .put("Name", "testOpportunity-1")
      .put("Amount", "25000")
      .build();

    List<SObjectDescriptor.FieldDescriptor> fieldDescriptors =
      getFieldDescriptors("Name", "Id", "Campaign.Name", "Campaign.Id");
    thrown.expect(IllegalStateException.class);

    SObjectDescriptor sObjectDescriptor = Mockito.mock(SObjectDescriptor.class);

    Mockito.when(sObjectDescriptor.getChildSObjects()).thenReturn(Collections.emptyList());
    Mockito.when(sObjectDescriptor.getFields()).thenReturn(fieldDescriptors);

    transformer.transformToMap(opportunity, sObjectDescriptor);
  }

  private List<SObjectDescriptor.FieldDescriptor> getFieldDescriptors(String... fields) {
    return Stream.of(fields)
      .map(name -> name.split("\\" + SalesforceConstants.REFERENCE_NAME_DELIMITER))
      .map(Arrays::asList)
      .map(nameParts -> new SObjectDescriptor.FieldDescriptor(nameParts, null, SalesforceFunctionType.NONE))
      .collect(Collectors.toList());
  }
}

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

import com.sforce.soap.partner.Address;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XmlObjectWrapper;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.etl.SObjectBuilder;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.namespace.QName;

public class SalesforceWideRecordReaderTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testTransformToMap() {
    SalesforceWideRecordReader recordReader = new SalesforceWideRecordReader(null, null);
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
    Map<String, String> resultMap = recordReader.transformToMap(opportunity, fieldDescriptors);

    Assert.assertNotNull(resultMap);
    Assert.assertEquals(fieldDescriptors.size(), resultMap.size());

    Assert.assertEquals(opportunity.getField("Name"), resultMap.get("Name"));
    Assert.assertEquals(opportunity.getField("Id"), resultMap.get("Id"));
    Assert.assertEquals(campaign.getField("Name"), resultMap.get("Campaign.Name"));
    Assert.assertEquals(campaign.getField("Id"), resultMap.get("Campaign.Id"));
  }

  @Test
  public void testTransformToMapCompoundField() throws JSONException {
    SalesforceWideRecordReader recordReader = new SalesforceWideRecordReader(null, null);

    Address address = new Address();
    address.setCity("New York");
    address.setCountry("US");
    address.setPostalCode("10001");
    address.setState("NY");

    XmlObjectWrapper compoundFieldBillingAddress = new XmlObjectWrapper(address);
    compoundFieldBillingAddress.setName(QName.valueOf("BillingAddress"));

    SObject opportunity = new SObjectBuilder()
      .setType("Opportunity")
      .put("Id", "testOpportunityId")
      .put("Name", "testOpportunity-1")
      .put("Amount", "25000")
      .put(compoundFieldBillingAddress.getName().getLocalPart(), compoundFieldBillingAddress)
      .build();

    List<SObjectDescriptor.FieldDescriptor> fieldDescriptors =
      getFieldDescriptors("Id", "BillingAddress");
    Map<String, String> resultMap = recordReader.transformToMap(opportunity, fieldDescriptors);

    Assert.assertNotNull(resultMap);
    Assert.assertEquals(fieldDescriptors.size(), resultMap.size());

    Assert.assertEquals(opportunity.getField("Id"), resultMap.get("Id"));

    String billingAddress = resultMap.get("BillingAddress");
    Assert.assertNotNull(billingAddress);
    // check compound field json
    JSONObject jsonObject = new JSONObject(billingAddress);
    Assert.assertEquals(address.getCity(), jsonObject.getString("city"));
    Assert.assertEquals(address.getCountry(), jsonObject.getString("country"));
    Assert.assertEquals(address.getPostalCode(), jsonObject.getString("postalCode"));
    Assert.assertEquals(address.getState(), jsonObject.getString("state"));

    Assert.assertTrue(jsonObject.isNull("street"));
    Assert.assertTrue(jsonObject.isNull("latitude"));
    Assert.assertTrue(jsonObject.isNull("longitude"));
  }


  @Test
  public void testTransformToMapIncorrectReferenceField() {
    SalesforceWideRecordReader recordReader = new SalesforceWideRecordReader(null, null);
    SObject opportunity = new SObjectBuilder()
      .setType("Opportunity")
      .put("Id", "testOpportunityId")
      .put("Name", "testOpportunity-1")
      .put("Amount", "25000")
      .build();

    List<SObjectDescriptor.FieldDescriptor> fieldDescriptors =
      getFieldDescriptors("Name", "Id", "Campaign.Name", "Campaign.Id");
    thrown.expect(IllegalStateException.class);

    recordReader.transformToMap(opportunity, fieldDescriptors);
  }

  private List<SObjectDescriptor.FieldDescriptor> getFieldDescriptors(String... fields) {
    return Stream.of(fields)
      .map(name -> name.split("\\" + SalesforceConstants.REFERENCE_NAME_DELIMITER))
      .map(Arrays::asList)
      .map(SObjectDescriptor.FieldDescriptor::new)
      .collect(Collectors.toList());
  }
}

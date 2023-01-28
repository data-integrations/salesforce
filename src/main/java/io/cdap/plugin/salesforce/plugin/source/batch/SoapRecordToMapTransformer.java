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
import com.sforce.ws.bind.XmlObject;
import io.cdap.plugin.salesforce.SObjectDescriptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Transformer class to transform Salesforce SObject to {@link Map}.
 */
public class SoapRecordToMapTransformer {

  private static final String SUB_QUERY_FIELDS_PARENT = "records";

  /**
   * Transforms SOAP API SObject to map for provided field names.
   *
   * @param sObject           SObject to be transformed
   * @param sObjectDescriptor SObject metadata to be used for fields extraction
   * @return map of fields names and values extracted from SObject
   */
  public Map<String, ?> transformToMap(SObject sObject, SObjectDescriptor sObjectDescriptor) {
    Map<String, Object> result = new HashMap<>(transformRowToMap(sObject, sObjectDescriptor));

    for (SObjectDescriptor childSObjectDescriptor : sObjectDescriptor.getChildSObjects()) {
      XmlObject child = sObject.getChild(childSObjectDescriptor.getName());
      if (child == null) {
        throw new IllegalStateException(
          String.format("SObject sub-query field with name '%s' not found in parent '%s'",
                        childSObjectDescriptor.getName(), sObject.getName().getLocalPart()));
      }
      Iterable<XmlObject> subValues = () -> child.getChildren(SUB_QUERY_FIELDS_PARENT);

      List<Map<String, String>> subQueryValues = StreamSupport.stream(subValues.spliterator(), false)
        .map(subValue -> transformRowToMap(subValue, childSObjectDescriptor))
        .collect(Collectors.toList());
      result.put(childSObjectDescriptor.getName(), subQueryValues);
    }
    return result;
  }

  public Map<String, String> transformRowToMap(XmlObject sObject, SObjectDescriptor sObjectDescriptor) {
    Map<String, String> result = new HashMap<>(sObjectDescriptor.getFields().size());
    for (SObjectDescriptor.FieldDescriptor fieldDescriptor : sObjectDescriptor.getFields()) {
      Object fieldValue;
      if (fieldDescriptor.getAlias() != null) {
        fieldValue = extractValue(sObject, fieldDescriptor.getAlias(), Collections.emptyList());
      } else {
        fieldValue = extractValue(sObject, fieldDescriptor.getName(), fieldDescriptor.getParents());
      }
      result.put(fieldDescriptor.getQueryName(), fieldValue == null ? null : String.valueOf(fieldValue));
    }
    return result;
  }

  /**
   * Extracts value from XmlObject field. Reference type fields extracted recursively.
   * <p/>
   * Example: `SELECT Id, Name, Campaign.Id FROM Opportunity LIMIT 1`
   * <p/>
   * Response:
   * <pre>
   * XmlObject{name=Opportunity, value=null,
   *   children=[
   *     XmlObject{name=Id, value=oid-1, children=[]},
   *     XmlObject{name=Name, value=value1, children=[]},
   *     XmlObject{name=Campaign, value=null, children=[XmlObject{name=Id, value=cid-1, children=[]}]}
   *   ]
   * }
   * </pre>
   * <ul>
   * <li>Extract simple field  `Id` from SObject Opportunity:
   * name=`Id`, children=`empty List()` -> `oid-1`</li>
   * <li>Extract simple field  `Name` from SObject Opportunity:
   * name=`Name`, children=`empty List()` -> `value1`</li>
   * <li>Extract reference field  `Campaign.Id` from SObject Opportunity:
   * name=`Id`, children=`List(Campaign)` -> `cid-1`</li>
   * </ul>
   *
   * @param xmlObject field value holder
   * @param name      field name
   * @param children  field's children names
   * @return field value
   */
  private Object extractValue(XmlObject xmlObject, String name, List<String> children) {
    if (children.isEmpty()) {
      return xmlObject.getField(name);
    }
    String childName = children.get(0);
    XmlObject child = xmlObject.getChild(childName);
    if (child == null) {
      throw new IllegalStateException(
        String.format("SObject reference field with name '%s' not found in parent '%s'",
                      childName, xmlObject.getName().getLocalPart()));
    }
    // remove higher level child from list and check remaining
    return extractValue(child, name, children.subList(1, children.size()));
  }
}

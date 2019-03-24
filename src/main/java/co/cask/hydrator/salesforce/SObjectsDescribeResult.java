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
package co.cask.hydrator.salesforce;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Retrieves {@link DescribeSObjectResult}s for the given sObjects
 * and adds field information to the internal holder.
 * This class will be used to populate {@link SObjectDescriptor} for queries by sObject
 * or to generate CDAP schema based on Salesforce fields information.
 */
public class SObjectsDescribeResult {

  // key -> [sObject name], value -> [key -> field name,  value -> field]
  private final Map<String, Map<String, Field>> holder = new HashMap<>();

  public SObjectsDescribeResult(PartnerConnection connection, Collection<String> sObjects) {
    // Salesforce limitation that we can describe only 100 sObjects at a time
    // split the given sObjects into smaller partitions to ensure we don't exceed the limitation
    Lists.partition(new ArrayList<>(sObjects), 100).stream()
      .map(partition -> {
        try {
          return connection.describeSObjects(partition.toArray(new String[0]));
        } catch (ConnectionException e) {
          throw new RuntimeException(e);
        }
      })
      .flatMap(Arrays::stream)
      .forEach(this::addSObjectDescribe);
  }

  @VisibleForTesting
  SObjectsDescribeResult(Map<String, Map<String, Field>> holder) {
    holder.forEach((key, value) -> this.holder.put(key.toLowerCase(), value));
  }

  /**
   * Retrieves all stored fields.
   *
   * @return list of {@link Field}s
   */
  public List<Field> getFields() {
    return holder.values().stream()
      .map(Map::values)
      .flatMap(Collection::stream)
      .collect(Collectors.toList());
  }

  /**
   * Attempts to find {@link Field} by sObject name and field name.
   *
   * @param sObjectName sObject name
   * @param fieldName field name
   * @return field instance if found, null otherwise
   */
  public Field getField(String sObjectName, String fieldName) {
    Map<String, Field> fields = holder.get(sObjectName.toLowerCase());
    return fields == null ? null : fields.get(fieldName);
  }

  private void addSObjectDescribe(DescribeSObjectResult sObjectDescribe) {
    Map<String, Field> fields = Arrays.stream(sObjectDescribe.getFields())
      .collect(Collectors.toMap(
        Field::getName,
        Function.identity(),
        (o, n) -> n,
        LinkedHashMap::new)); // preserve field order for queries by sObject
    // sObjects names are case-insensitive
    // store them in lower case to ensure we obtain them case-insensitively
    holder.put(sObjectDescribe.getName().toLowerCase(), fields);
  }
}

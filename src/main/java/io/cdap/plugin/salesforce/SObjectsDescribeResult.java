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
package io.cdap.plugin.salesforce;

import com.google.common.collect.Lists;
import com.sforce.soap.partner.ChildRelationship;
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
import java.util.stream.Stream;

/**
 * Retrieves {@link DescribeSObjectResult}s for the given sObjects
 * and adds field information to the internal holder.
 * This class will be used to populate {@link SObjectDescriptor} for queries by sObject
 * or to generate CDAP schema based on Salesforce fields information.
 */
public class SObjectsDescribeResult {

  // Salesforce limitation that we can describe only 100 sObjects at a time
  private static final int DESCRIBE_SOBJECTS_LIMIT = 100;

  // key -> [sObject name], value -> [key -> field name,  value -> field]
  private final Map<String, Map<String, Field>> objectToFieldMap;

  /**
   * Describes list of given SObjects and retrieves information about each SObjects fields types.
   *
   * @param connection Salesforce partner connection
   * @param sObjects list of SObjects to be described
   * @return SObjectsDescribeResult instance
   * @throws ConnectionException when unable to connect to Salesforce
   */
  public static SObjectsDescribeResult of(PartnerConnection connection, Collection<String> sObjects)
    throws ConnectionException {
    Map<String, Map<String, Field>> objectToFieldMap = new HashMap<>();
    // split the given sObjects into smaller partitions to ensure we don't exceed the limitation
    for (List<String> partition : Lists.partition(new ArrayList<>(sObjects), DESCRIBE_SOBJECTS_LIMIT)) {
      DescribeSObjectResult[] describeSObjectResults = connection.describeSObjects(partition.toArray(new String[0]));
      Stream.of(describeSObjectResults)
        .forEach(result -> addSObjectDescribe(result.getName(), result.getFields(), objectToFieldMap));
    }
    return new SObjectsDescribeResult(objectToFieldMap);
  }

  /**
   * Describes given top level SObject and its featured SObjects, i.e. SObjects
   * which were present in the query due to parent-to-child or child-to-parent relationship usage.
   *
   * @param connection Salesforce partner connection
   * @param name top level SObject name
   * @param featuredSObjectsCombinations unique combinations of featured SObjects
   * @return SObjectsDescribeResult instance
   * @throws ConnectionException when unable to connect to Salesforce
   */
  public static SObjectsDescribeResult of(PartnerConnection connection, String name,
                                          Collection<SObjectDescriptor.FeaturedSObjects> featuredSObjectsCombinations)
    throws ConnectionException {
    Map<String, Map<String, Field>> objectToFieldMap = new HashMap<>();
    Map<String, DescribeSObjectResult> cache = new HashMap<>();
    // describe top level SObject first and store it to be used during schema creation
    DescribeSObjectResult topLevelDescribe = describe(connection, name, cache);
    addSObjectDescribe(name, topLevelDescribe.getFields(), objectToFieldMap);

    // describe each featured SObjects combination
    for (SObjectDescriptor.FeaturedSObjects featuredSObjects : featuredSObjectsCombinations) {
      // at the beginning last describe is top level SObject describe result
      DescribeSObjectResult lastDescribe = topLevelDescribe;

        /*
          If featured SObjects have relationship, describe it first.
          For the query `SELECT Name, (SELECT LastName, Owner.UserRole.Id FROM Contacts) FROM Account`
          `Contacts` is relationship SObject.
         */
      if (featuredSObjects.hasRelationship()) {
        String relationshipName = getRelationshipName(featuredSObjects.getRelationship(), lastDescribe);
        if (relationshipName == null) {
          throw new IllegalArgumentException(
            String.format("Relationship field name '%s' is absent in SObject '%s' describe result",
              featuredSObjects.getRelationship(), lastDescribe.getName()));
        }
        lastDescribe = describe(connection, relationshipName, cache);
      }

        /*
          If featured SObjects have references, describe describe them as well.
          For the query `SELECT Name, (SELECT LastName, Owner.UserRole.Id FROM Contacts) FROM Account`
          `Owner` and `UserRole` are reference SObjects.
         */
      if (featuredSObjects.hasReferences()) {
        for (String reference : featuredSObjects.getReferences()) {
          String referenceName = getReferenceName(reference, lastDescribe);
          if (referenceName == null) {
            throw new IllegalArgumentException(
              String.format("Reference field name '%s' is absent in SObject '%s' describe result",
                reference, lastDescribe.getName()));
          }
          lastDescribe = describe(connection, referenceName, cache);
        }
      }

      // store final describe result to be used during schema creation
      addSObjectDescribe(featuredSObjects.getFullName(name), lastDescribe.getFields(), objectToFieldMap);
    }
    return new SObjectsDescribeResult(objectToFieldMap);
  }

  public static SObjectsDescribeResult of(Map<String, Map<String, Field>> holder) {
    Map<String, Map<String, Field>> objectToFieldMap = new HashMap<>();
    for (Map.Entry<String, Map<String, Field>> holderEntry : holder.entrySet()) {
      String sObjectName = holderEntry.getKey();
      Map<String, Field> fieldsMap = holderEntry.getValue();

      Map<String, Field> fieldsMapLowerCase = new LinkedHashMap<>();
      fieldsMap.forEach((key, value) -> fieldsMapLowerCase.put(key.toLowerCase(), value));

      objectToFieldMap.put(sObjectName.toLowerCase(), fieldsMapLowerCase);
    }
    return new SObjectsDescribeResult(objectToFieldMap);
  }

  private SObjectsDescribeResult(Map<String, Map<String, Field>> objectToFieldMap) {
    this.objectToFieldMap = objectToFieldMap;
  }

  /**
   * Retrieves all stored fields.
   *
   * @return list of {@link Field}s
   */
  public List<Field> getFields() {
    return objectToFieldMap.values().stream()
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
    Map<String, Field> fields = objectToFieldMap.get(sObjectName.toLowerCase());
    return fields == null ? null : fields.get(fieldName.toLowerCase());
  }

  private static void addSObjectDescribe(String name,
                                         Field[] result,
                                         Map<String, Map<String, Field>> objectToFieldMap) {
    Map<String, Field> fields = Arrays.stream(result)
      .collect(Collectors.toMap(
        field -> field.getName().toLowerCase(),
        Function.identity(),
        (o, n) -> n,
        LinkedHashMap::new)); // preserve field order for queries by sObject

    // sObjects names are case-insensitive
    // store them in lower case to ensure we obtain them case-insensitively
    objectToFieldMap.put(name.toLowerCase(), fields);
  }

  private static String getReferenceName(String name, DescribeSObjectResult result) {
    return result == null
      ? null
      : Stream.of(result.getFields())
      .filter(field -> name.equals(field.getRelationshipName()))
      .findAny()
      .map(field -> field.getReferenceTo()[0]) // only first reference name will be used
      .orElse(null);
  }

  private static String getRelationshipName(String name, DescribeSObjectResult result) {
    return result == null
      ? null
      : Stream.of(result.getChildRelationships())
      .filter(childRelationship -> name.equals(childRelationship.getRelationshipName()))
      .findAny()
      .map(ChildRelationship::getChildSObject)
      .orElse(null);
  }

  private static DescribeSObjectResult describe(PartnerConnection connection, String name,
                                         Map<String, DescribeSObjectResult> cache)
    throws ConnectionException {
    DescribeSObjectResult describe = cache.get(name.toLowerCase());
    // if SObject describe result is absent in cache, try to obtain it from Salesforce
    if (describe == null) {
      describe = connection.describeSObject(name);
      if (describe == null) {
        throw new IllegalArgumentException("Unable to describe SObject: " + name);
      }
    }
    // store describe result in cache for future re-use
    cache.put(name.toLowerCase(), describe);
    return describe;
  }

  public static boolean isCustomObject(PartnerConnection connection, String name) throws ConnectionException {
    return describe(connection, name, new HashMap<>()).isCustom();
  }
}

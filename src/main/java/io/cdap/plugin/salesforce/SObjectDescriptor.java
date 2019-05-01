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

import com.google.common.base.Preconditions;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.parser.SalesforceQueryParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Contains information about SObject, including its name and list of fields.
 * Can be obtained from SOQL query or from SObject name.
 */
public class SObjectDescriptor {

  private final String name;
  private final List<FieldDescriptor> fields;

  /**
   * Connects to Salesforce, gets describe result for the given sObject name and stores
   * information about its fields into {@link SObjectDescriptor} class.
   *
   * @param name sObject name
   * @param credentials Salesforce connection credentials
   * @param typesToSkip sobject fields of this type will be skipped.
   * @return sObject descriptor
   * @throws ConnectionException in case of errors when establishing connection to Salesforce
   */
  public static SObjectDescriptor fromName(String name,
                                           AuthenticatorCredentials credentials, Set<FieldType> typesToSkip)
    throws ConnectionException {
    PartnerConnection partnerConnection = SalesforceConnectionUtil.getPartnerConnection(credentials);
    SObjectsDescribeResult describeResult = new SObjectsDescribeResult(
      partnerConnection, Collections.singletonList(name));
    List<FieldDescriptor> fields = describeResult.getFields().stream()
      .filter(field -> !typesToSkip.contains(field.getType()))
      .map(FieldDescriptor::new)
      .collect(Collectors.toList());

    return new SObjectDescriptor(name, fields);
  }

  public static SObjectDescriptor fromName(String name,
                                           AuthenticatorCredentials credentials) throws ConnectionException {
    return fromName(name, credentials, Collections.emptySet());
  }

  /**
   * Parses given SOQL query and stores information about sObject name and its fields into
   * {@link SObjectDescriptor} class.
   *
   * @param query SOQL query
   * @return sObject descriptor
   */
  public static SObjectDescriptor fromQuery(String query) {
    return SalesforceQueryParser.getObjectDescriptorFromQuery(query);
  }

  public SObjectDescriptor(String name, List<FieldDescriptor> fields) {
    this.name = name;
    this.fields = new ArrayList<>(fields);
  }

  public String getName() {
    return name;
  }

  /**
   * Collects sObject names needed to be described in order to obtains field type information.
   *
   * @return list of sObject names
   */
  public Set<String> getAllParentObjects() {
    Set<String> parents = fields.stream()
      .filter(FieldDescriptor::hasParents)
      .map(FieldDescriptor::getLastParent)
      .collect(Collectors.toSet());

    // add top level sObject for fields that don't have parents
    parents.add(name);

    return parents;
  }

  /**
   * Collects all field names, for fields with parents includes parents separated by dot.
   *
   * @return list of field names
   */
  public List<String> getFieldsNames() {
    return fields.stream()
      .map(FieldDescriptor::getFullName)
      .collect(Collectors.toList());
  }

  public List<FieldDescriptor> getFields() {
    return fields;
  }

  @Override
  public String toString() {
    return "SObjectDescriptor{" + "name='" + name + '\'' + ", fields=" + fields + '}';
  }

  /**
   * Contains information about field, including list of parents if present.
   */
  public static class FieldDescriptor {

    private final Field field;
    private final List<String> parents;

    public FieldDescriptor(Field field) {
      this.field = field;
      this.parents = new ArrayList<>();
    }

    public FieldDescriptor(List<String> nameParts) {
      Preconditions.checkState(nameParts != null && !nameParts.isEmpty(),
        "Given list of name parts must contain at least one element");
      this.parents = new ArrayList<>(nameParts);
      this.field = new Field();
      field.setName(parents.remove(nameParts.size() - 1));
    }

    public String getName() {
      return field.getName();
    }

    /**
     * Returns field name with parents connected by dots.
     *
     * @return full field name
     */
    public String getFullName() {
      if (hasParents()) {
        List<String> nameParts = new ArrayList<>(parents);
        nameParts.add(field.getName());
        return String.join(SalesforceConstants.REFERENCE_NAME_DELIMITER, nameParts);
      }
      return field.getName();
    }

    /**
     * Checks if field has parents.
     *
     * @return true if field has at least one parent, false otherwise
     */
    public boolean hasParents() {
      return !parents.isEmpty();
    }

    public List<String> getParents() {
      return parents;
    }

    /**
     * Return last parent of the field.
     * Primary used to obtain describe result from Salesforce.
     *
     * @return last parent if field has parents, null otherwise
     */
    public String getLastParent() {
      return hasParents() ? parents.get(parents.size() - 1) : null;
    }

    @Nullable
    public FieldType getFieldType() {
      return field.getType();
    }

    @Override
    public String toString() {
      return "FieldDescriptor{" + "name='" + field.getName() + '\'' + ", parents=" + parents + '}';
    }
  }

}

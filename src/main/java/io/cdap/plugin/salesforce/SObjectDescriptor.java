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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Contains information about SObject, including its name, list of fields and sub-query elements.
 * Can be obtained from SOQL query or from SObject name.
 */
public class SObjectDescriptor {

  private final String name;
  private final List<FieldDescriptor> fields;
  private final List<SObjectDescriptor> childSObjects;

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
    SObjectsDescribeResult describeResult = SObjectsDescribeResult.of(
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
    this(name, fields, Collections.emptyList());
  }

  public SObjectDescriptor(String name, List<FieldDescriptor> fields, List<SObjectDescriptor> childSObjects) {
    this.name = name;
    this.fields = new ArrayList<>(fields);
    this.childSObjects = childSObjects;
  }

  public String getName() {
    return name;
  }

  /**
   * Collects unique combinations of featured SObjects.
   *
   * For the query
   * `SELECT DandbCompany.Name, (SELECT FirstName, LastName, Owner.UserRole.Id FROM Contacts) FROM Account`
   * there are three unique combinations of features SObjects:
   * Name -> FeaturedSObjects{relationship=null, references=[DandbCompany]}
   * FirstName, LastName -> FeaturedSObjects{relationship='Contacts', references=[]}
   * Id -> FeaturedSObjects{relationship='Contacts', references=[Owner, UserRole]}
   *
   * @return unique combinations of featured SObjects
   */
  public Set<FeaturedSObjects> getFeaturedSObjects() {
    Set<FeaturedSObjects> result = getFeaturedSObjects(null, getFields());

    for (SObjectDescriptor child : getChildSObjects()) {
      result.add(new FeaturedSObjects(child.getName(), Collections.emptyList()));
      result.addAll(getFeaturedSObjects(child.getName(), child.getFields()));
    }

    return result;
  }

  private Set<FeaturedSObjects> getFeaturedSObjects(String name, List<FieldDescriptor> fields) {
    return fields.stream()
      .filter(FieldDescriptor::hasParents)
      .map(f -> new FeaturedSObjects(name, f.getParents()))
      .collect(Collectors.toSet());
  }

  /**
   * Collects all field names, for fields with parents, parents are separated by dot.
   *
   * @return list of field names
   */
  public List<String> getFieldsNames() {
    return fields.stream()
      .map(FieldDescriptor::getQueryName)
      .collect(Collectors.toList());
  }

  public List<FieldDescriptor> getFields() {
    return fields;
  }

  public List<SObjectDescriptor> getChildSObjects() {
    return childSObjects;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, fields, childSObjects);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SObjectDescriptor that = (SObjectDescriptor) o;
    return name.equals(that.name)
      && fields.equals(that.fields)
      && childSObjects.equals(that.childSObjects);
  }

  @Override
  public String toString() {
    return "SObjectDescriptor{"
      + "name=" + name
      + ", fields=" + fields
      + ", childSObjects=" + childSObjects
      + '}';
  }

  /**
   * Contains information about field, including list of parents if present.
   * Can contain field alias and function type if field is used with function call.
   */
  public static class FieldDescriptor {

    private final Field field;
    private final List<String> parents;
    @Nullable
    private final String alias;
    private final SalesforceFunctionType functionType;

    public FieldDescriptor(Field field) {
      this.field = field;
      this.parents = new ArrayList<>();
      this.alias = null;
      this.functionType = SalesforceFunctionType.NONE;
    }

    /**
     * Creates field descriptor instance based on given field information.
     * <ul>
     * <li>`nameParts` contains list of field names.
     * For simple field it will contain singleton list with field name only (SELECT Id FROM Opportunity).
     * For reference fields each list element will correspond to the element in the field name
     * initially separated by dot (SELECT Account.Name FROM Contact).</li>
     * <li>`alias` represents field alternative name given in the query.
     * Only applicable for queries with aggregate function calls.
     * If field does not have alias, its value is null.</li>
     * <li>`functionType` indicates function type for the fields used in function calls.
     * If field was not used in function call,
     * its function type will be {@link SalesforceFunctionType#NONE}.</li>
     * </ul>
     *
     * @param nameParts all field name parts
     * @param alias field alias
     * @param functionType function type for the fields used in function calls
     */
    public FieldDescriptor(List<String> nameParts, String alias,
                           SalesforceFunctionType functionType) {
      Preconditions.checkState(nameParts != null && !nameParts.isEmpty(),
        "Given list of name parts must contain at least one element");
      this.parents = new ArrayList<>(nameParts);
      this.field = new Field();
      field.setName(parents.remove(nameParts.size() - 1));

      this.alias = alias;
      this.functionType = functionType;
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
     * Checks if field has parents. Only reference fields have parents.
     * Example:
     * <ul>
     * <li>`SELECT Id from Opportunity`: `Id` field does not have parents.</li>
     * <li>`SELECT Account.Name FROM Contact`: `Name` field has one parent `Account`.</li>
     * </ul>
     *
     * @return true if field has at least one parent, false otherwise
     */
    public boolean hasParents() {
      return !parents.isEmpty();
    }

    public List<String> getParents() {
      return parents;
    }

    public String getParentsPath(List<String> topLevelParents) {
      List<String> allParents = new ArrayList<>(topLevelParents);
      allParents.addAll(parents);
      return String.join(SalesforceConstants.REFERENCE_NAME_DELIMITER, allParents);
    }

    @Nullable
    public FieldType getFieldType() {
      return field.getType();
    }

    public List<String> getNameParts() {
      List<String> nameParts = new ArrayList<>(parents);
      nameParts.add(field.getName());
      return nameParts;
    }

    /**
     * Checks if field has alias.
     *
     * @return true if field has alia, false otherwise
     */
    public boolean hasAlias() {
      return alias != null;
    }

    /**
     * Returns field alias if it has one.
     *
     * @return field alias
     */
    public String getAlias() {
      return alias;
    }

    /**
     * If fields name was used with alias, returns field alias,
     * otherwise returns full field name with parents if any.
     *
     * @return field query name
     */
    public String getQueryName() {
      return hasAlias() ? alias : getFullName();
    }

    /**
     * If field was used with function call returns used function type.
     *
     * @return function type
     */
    public SalesforceFunctionType getFunctionType() {
      return functionType;
    }

    @Override
    public int hashCode() {
      return Objects.hash(field.getName(), field.getType(), field.isNillable(),
        parents, alias, functionType);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FieldDescriptor that = (FieldDescriptor) o;
      return Objects.equals(field.getName(), that.field.getName())
        && field.getType() == that.field.getType()
        && field.isNillable() == that.field.isNillable()
        && parents.equals(that.parents)
        && Objects.equals(alias, that.alias)
        && functionType == that.functionType;
    }

    @Override
    public String toString() {
      return "FieldDescriptor{"
        + "fieldName=" + field.getName()
        + ", fieldType=" + field.getType()
        + ", nillable=" + field.isNillable()
        + ", parents=" + parents
        + ", alias=" + alias
        + ", functionType=" + functionType
        + '}';
    }
  }

  /**
   * Describes combination of SObjects used in the query for the field.
   * For the query
   * `SELECT DandbCompany.Name, (SELECT FirstName, LastName, Owner.UserRole.Id FROM Contacts) FROM Account`
   * there are three unique combinations of features SObjects:
   * Name -> FeaturedSObjects{relationship=null, references=[DandbCompany]}
   * FirstName, LastName -> FeaturedSObjects{relationship='Contacts', references=[]}
   * Id -> FeaturedSObjects{relationship='Contacts', references=[Owner, UserRole]}
   */
  public static class FeaturedSObjects {
    @Nullable
    private final String relationship;
    private final List<String> references;

    public FeaturedSObjects(String relationship, List<String> references) {
      this.relationship = relationship;
      this.references = references == null ? Collections.emptyList() : references;
    }

    public String getRelationship() {
      return relationship;
    }

    public List<String> getReferences() {
      return references;
    }

    public boolean hasRelationship() {
      return relationship != null;
    }

    public boolean hasReferences() {
      return !references.isEmpty();
    }

    public String getFullName(String topLevelName) {
      List<String> names = new ArrayList<>();
      names.add(topLevelName);
      if (hasRelationship()) {
        names.add(relationship);
      }
      if (hasReferences()) {
        names.addAll(references);
      }
      return String.join(SalesforceConstants.REFERENCE_NAME_DELIMITER, names);
    }

    @Override
    public int hashCode() {
      return Objects.hash(relationship, references);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FeaturedSObjects obj = (FeaturedSObjects) o;
      return Objects.equals(relationship, obj.relationship) && Objects.equals(references, obj.references);
    }

    @Override
    public String toString() {
      return "FeaturedSObjects{"
        + "relationship=" + relationship
        + ", references=" + references
        + '}';
    }
  }
}

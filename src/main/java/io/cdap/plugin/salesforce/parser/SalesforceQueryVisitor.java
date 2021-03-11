/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.cdap.plugin.salesforce.parser;

import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceFunctionType;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.misc.Interval;
import soql.SOQLBaseVisitor;
import soql.SOQLParser;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Visitor which validates and parses query statement and retrieves top sObject and its fields information.
 */
public class SalesforceQueryVisitor extends SOQLBaseVisitor<SObjectDescriptor> {

  @Override
  public SObjectDescriptor visitStatement(SOQLParser.StatementContext ctx) {
    SOQLParser.ObjectTypeContext objectTypeContext = ctx.fromStatement().objectType();
    // retrieve alias to use it later to exclude from field name
    String alias = objectTypeContext.alias() == null ? null : objectTypeContext.alias().getText();

    if (objectTypeContext.id_or_keyword() == null || objectTypeContext.id_or_keyword().size() != 1) {
      throw new SOQLParsingException("Invalid top level object: " + ctx.getText());
    }

    String name = objectTypeContext.id_or_keyword().get(0).getText();

    SOQLParser.FieldListContext fieldListContext = ctx.fieldList();

    List<SObjectDescriptor.FieldDescriptor> fields = fieldListContext
      .accept(new FieldListVisitor(name, alias, false));

    if (fields.isEmpty()) {
      throw new SOQLParsingException("SOQL must have at least one field: " + ctx.getText());
    }
    if (ctx.fromStatement().GROUP() == null) {
      // Salesforce allows aliases only with aggregate expressions
      List<String> aliasedFields = fields.stream()
        .filter(fieldDescriptor -> fieldDescriptor.getFunctionType() == SalesforceFunctionType.NONE)
        .filter(SObjectDescriptor.FieldDescriptor::hasAlias)
        .map(SObjectDescriptor.FieldDescriptor::getFullName)
        .collect(Collectors.toList());

      if (!aliasedFields.isEmpty()) {
        throw new SOQLParsingException("Field aliasing is not allowed except when using a GROUP BY clause. "
          + "List of fields with alias: " + aliasedFields);
      }
    } else {
      List<String> unAliasedFields = fields.stream()
        .filter(fieldDescriptor -> !fieldDescriptor.hasAlias())
        .filter(SObjectDescriptor.FieldDescriptor::hasParents)
        .map(SObjectDescriptor.FieldDescriptor::getFullName)
        .collect(Collectors.toList());

      if (!unAliasedFields.isEmpty()) {
        throw new SOQLParsingException("Reference fields must have an alias when using a GROUP BY clause. "
          + "List of reference fields without alias: " + unAliasedFields);
      }
    }

    List<SObjectDescriptor> childSObjects = fieldListContext.accept(new SubQueryListVisitor());

    return new SObjectDescriptor(name, fields, childSObjects);
  }

  /**
   * Visits fields indicated in the select statement and retrieves their information.
   */
  private class FieldListVisitor extends SOQLBaseVisitor<List<SObjectDescriptor.FieldDescriptor>> {

    private final String objectName;
    private final String objectAlias;
    private final boolean subQueryScope;

    FieldListVisitor(String objectName, String objectAlias, boolean subQueryScope) {
      this.objectName = objectName;
      this.objectAlias = objectAlias;
      this.subQueryScope = subQueryScope;
    }

    @Override
    public List<SObjectDescriptor.FieldDescriptor> visitStarElement(SOQLParser.StarElementContext ctx) {
      throw new SOQLParsingException("Star queries are not supported: " + ctx.getText());
    }

    @Override
    public List<SObjectDescriptor.FieldDescriptor> visitFieldElements(SOQLParser.FieldElementsContext ctx) {
      FieldVisitor fieldVisitor = new FieldVisitor(objectName, objectAlias, true, subQueryScope);
      return ctx.fieldElement().stream()
        .map(f -> f.accept(fieldVisitor))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
    }
  }

  /**
   * Visits field and returns {@link SObjectDescriptor.FieldDescriptor} instance based on field information.
   * Will fail if encountered unsupported field types.
   */
  private class FieldVisitor extends SOQLBaseVisitor<SObjectDescriptor.FieldDescriptor> {

    private final String objectName;
    private final String objectAlias;
    private final boolean requireAlias;
    private final boolean subQueryScope;

    FieldVisitor(String objectName, String objectAlias, boolean requireAlias, boolean subQueryScope) {
      this.objectName = objectName;
      this.objectAlias = objectAlias;
      this.requireAlias = requireAlias;
      this.subQueryScope = subQueryScope;
    }

    @Override
    public SObjectDescriptor.FieldDescriptor visitFieldName(SOQLParser.FieldNameContext ctx) {
      List<String> nameParts = ctx.field().id_or_keyword().stream()
        .map(RuleContext::getText)
        .collect(Collectors.toList());

      if (nameParts.isEmpty()) {
        throw new SOQLParsingException("Invalid column name: " + ctx.getText());
      }

      if (nameParts.size() > 1) {
        // if field contains alias, remove it to get actual field name
        // Example:
        // select Opportunity.Name from Opportunity -> Name
        // select o.Name from Opportunity o -> Name
        String topParent = nameParts.get(0);
        if (topParent.equalsIgnoreCase(objectName) || topParent.equals(objectAlias)) {
          // remove alias
          nameParts = nameParts.subList(1, nameParts.size());
        }
      }

      String alias = ctx.alias() == null ? null : ctx.alias().getText();
      return new SObjectDescriptor.FieldDescriptor(nameParts, alias, SalesforceFunctionType.NONE);
    }

    @Override
    public SObjectDescriptor.FieldDescriptor visitSubquery(SOQLParser.SubqueryContext ctx) {
      if (subQueryScope) {
        throw new SOQLParsingException("Sub-queries are not supported inside of other sub-queries: "
          + ctx.getText());
      }
      return null;
    }

    @Override
    public SObjectDescriptor.FieldDescriptor visitFunctionCall(SOQLParser.FunctionCallContext ctx) {
      if (subQueryScope) {
        throw new SOQLParsingException("Function calls are not supported in sub-queries: " + ctx.getText());
      }

      SOQLParser.AliasContext aliasContext = ctx.alias();
      String alias = aliasContext == null ? null : aliasContext.getText();
      if (requireAlias && alias == null) {
        throw new SOQLParsingException("Function call must have alias: " + ctx.getText());
      }

      SOQLParser.FunctionContext function = ctx.function();
      String functionName = function.functionName().getText();
      SalesforceFunctionType functionType = SalesforceFunctionType.get(functionName);

      if (functionType.isConstant()) {
        return new SObjectDescriptor.FieldDescriptor(Collections.singletonList(alias), alias, functionType);
      }

      SOQLParser.FieldElementContext fieldElementContext = function.fieldElement();
      FieldVisitor visitor = new FieldVisitor(objectName, objectAlias, false, false);

      SObjectDescriptor.FieldDescriptor result = fieldElementContext.accept(visitor);
      if (result == null) {
        throw new SOQLParsingException("Function must be applied to the named field: " + ctx.getText());
      }

      return new SObjectDescriptor.FieldDescriptor(result.getNameParts(), alias, functionType);
    }

    @Override
    public SObjectDescriptor.FieldDescriptor visitTypeOfClause(SOQLParser.TypeOfClauseContext ctx) {
      throw new SOQLParsingException("TypeOf clauses are not supported: " + ctx.getText());
    }
  }

  /**
   * Visits sub-queries and returns list of child SObjects.
   */
  private class SubQueryListVisitor extends SOQLBaseVisitor<List<SObjectDescriptor>> {

    @Override
    public List<SObjectDescriptor> visitStarElement(SOQLParser.StarElementContext ctx) {
      throw new SOQLParsingException("Star queries are not supported in sub-queries: " + ctx.getText());
    }

    @Override
    public List<SObjectDescriptor> visitFieldElements(SOQLParser.FieldElementsContext ctx) {
      SubQueryVisitor visitor = new SubQueryVisitor();
      return ctx.fieldElement().stream()
        .map(f -> f.accept(visitor))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
    }
  }

  /**
   * Visits sub-query fields and returns {@link SObjectDescriptor} representing child SObject.
   */
  private class SubQueryVisitor extends SOQLBaseVisitor<SObjectDescriptor> {

    @Override
    public SObjectDescriptor visitSubquery(SOQLParser.SubqueryContext ctx) {
      SOQLParser.ObjectTypeContext objectTypeContext = ctx.objectType();
      if (objectTypeContext.id_or_keyword() == null || objectTypeContext.id_or_keyword().size() != 1) {
        throw new SOQLParsingException("Invalid top level object: " + ctx.getText());
      }

      String name = objectTypeContext.id_or_keyword().get(0).getText();
      String alias = objectTypeContext.alias() == null ? null : objectTypeContext.alias().getText();

      SOQLParser.FieldListContext fieldListContext = ctx.fieldList();

      FieldListVisitor visitor = new FieldListVisitor(name, alias, true);
      List<SObjectDescriptor.FieldDescriptor> fields = fieldListContext.accept(visitor);

      return new SObjectDescriptor(name, fields);
    }
  }

  /**
   * Visits query statement and extracts from statement in the original representation.
   */
  public static class FromStatementVisitor extends SOQLBaseVisitor<String> {

    @Override
    public String visitStatement(SOQLParser.StatementContext ctx) {
      SOQLParser.FromStatementContext fromStatementContext = ctx.fromStatement();
      Interval interval = new Interval(
        fromStatementContext.start.getStartIndex(),
        fromStatementContext.stop.getStopIndex());
      return fromStatementContext.start.getInputStream().getText(interval);
    }
  }

  /**
   * Visits query from statement and checks if it contains clauses that are restricted by Bulk API.
   * For example: GROUP BY [ROLLUP / CUBE], OFFSET.
   * Also checks if query contains restricted field types (function calls, sub-query fields).
   */
  public static class RestrictedQueryVisitor extends SOQLBaseVisitor<Boolean> {

    @Override
    public Boolean visitStatement(SOQLParser.StatementContext ctx) {

      SOQLParser.FromStatementContext fromStatementContext = ctx.fromStatement();
      if (fromStatementContext.GROUP() != null || fromStatementContext.OFFSET() != null) {
        return true;
      }

      return ctx.fieldList().accept(new RestrictedFieldVisitor());
    }
  }

  /**
   * Visits query fields that are restricted by Bulk API.
   * Returns true if at least one restricted field type is present, false otherwise.
   * For example: function calls, sub-queries.
   */
  public static class RestrictedFieldVisitor extends SOQLBaseVisitor<Boolean> {

    @Override
    public Boolean visitFunctionCall(SOQLParser.FunctionCallContext ctx) {
      return Boolean.TRUE;
    }

    @Override
    public Boolean visitSubquery(SOQLParser.SubqueryContext ctx) {
      return Boolean.TRUE;
    }

    @Override
    protected Boolean defaultResult() {
      return Boolean.FALSE;
    }
  }

  /**
   * Visits query fields that are restricted by Bulk API with PKChunk Enable.
   * Returns true if there is condition clause other than 'WHERE', false otherwise.
   * For example: SELECT Name FROM Opportunity LIMIT 10.
   */
  public static class RestrictedPKQueryVisitor extends SOQLBaseVisitor<Boolean> {

    @Override
    public Boolean visitStatement(SOQLParser.StatementContext ctx) {
      SOQLParser.FromStatementContext fromStatementContext = ctx.fromStatement();
      if (
        fromStatementContext.WITH() != null ||
          fromStatementContext.GROUP() != null ||
          fromStatementContext.ORDER() != null ||
          fromStatementContext.LIMIT() != null ||
          fromStatementContext.OFFSET() != null ||
          fromStatementContext.FOR() != null) {
        return true;
      }

      return ctx.fieldList().accept(new RestrictedFieldVisitor());
    }
  }
}

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
package co.cask.hydrator.salesforce.parser;

import co.cask.hydrator.salesforce.SObjectDescriptor;
import org.antlr.v4.runtime.RuleContext;
import soql.SOQLBaseVisitor;
import soql.SOQLParser;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Visitor which validates and parses query statement and retrieves top sObject and its fields information.
 */
public class SalesforceQueryVisitor extends SOQLBaseVisitor<SObjectDescriptor> {

  @Override
  public SObjectDescriptor visitStatement(SOQLParser.StatementContext ctx) {
    List<SOQLParser.ObjectTypeContext> objectContexts = ctx.objectList().objectType();

    if (objectContexts.size() != 1) {
      throw new SOQLParsingException("Expecting only one top level object, instead received: "
        + ctx.objectList().getText());
    }

    SOQLParser.ObjectTypeContext objectTypeContext = objectContexts.get(0);
    // retrieve alias to use it later to exclude from field name
    String alias = objectTypeContext.alias() == null ? null : objectTypeContext.alias().getText();

    if (objectTypeContext.id_or_keyword() == null || objectTypeContext.id_or_keyword().size() != 1) {
      throw new SOQLParsingException("Invalid top level object: " + ctx.getText());
    }

    String name = objectTypeContext.id_or_keyword().get(0).getText();

    List<SObjectDescriptor.FieldDescriptor> fields = ctx.fieldList()
      .accept(new FieldListVisitor(name, alias));

    if (fields.isEmpty()) {
      throw new SOQLParsingException("SOQL must have at least one field: " + ctx.getText());
    }

    return new SObjectDescriptor(name, fields);
  }

  /**
   * Visits fields indicated in the select statement and retrieves their information.
   */
  private class FieldListVisitor extends SOQLBaseVisitor<List<SObjectDescriptor.FieldDescriptor>> {

    private final String objectName;
    private final String objectAlias;

    FieldListVisitor(String objectName, String objectAlias) {
      this.objectName = objectName;
      this.objectAlias = objectAlias;
    }

    @Override
    public List<SObjectDescriptor.FieldDescriptor> visitStarElement(SOQLParser.StarElementContext ctx) {
      throw new SOQLParsingException("Star queries are not supported: " + ctx.getText());
    }

    @Override
    public List<SObjectDescriptor.FieldDescriptor> visitFieldElements(SOQLParser.FieldElementsContext ctx) {
      FieldVisitor fieldVisitor = new FieldVisitor(objectName, objectAlias);
      return ctx.fieldElement().stream()
        .map(f -> f.accept(fieldVisitor))
        .collect(Collectors.toList());
    }
  }

  /**
   * Visits field and returns {@link SObjectDescriptor.FieldDescriptor} instance based on field information.
   * Will fails if unsupported field types.
   */
  private class FieldVisitor extends SOQLBaseVisitor<SObjectDescriptor.FieldDescriptor> {

    private final String objectName;
    private final String objectAlias;

    FieldVisitor(String objectName, String objectAlias) {
      this.objectName = objectName;
      this.objectAlias = objectAlias;
    }

    @Override
    public SObjectDescriptor.FieldDescriptor visitFieldName(SOQLParser.FieldNameContext ctx) {
      List<String> nameParts = ctx.field().id_or_keyword().stream()
        .map(RuleContext::getText)
        .collect(Collectors.toList());

      if (nameParts.isEmpty()) {
        throw new SOQLParsingException("Invalid column name: " + ctx.getText());
      }

      // if field contains alias, remove it to get actual field name
      // Example:
      // select Opportunity.Name from Opportunity -> Name
      // select o.Name from Opportunity o -> Name
      String topParent = nameParts.get(0);
      if (topParent.equalsIgnoreCase(objectName) || topParent.equals(objectAlias)) {
        // remove alias
        nameParts = nameParts.subList(1, nameParts.size());
      }

      if (nameParts.isEmpty()) {
        throw new SOQLParsingException("Invalid column name: " + ctx.getText());
      }

      return new SObjectDescriptor.FieldDescriptor(nameParts);
    }

    @Override
    public SObjectDescriptor.FieldDescriptor visitSubquery(SOQLParser.SubqueryContext ctx) {
      throw new SOQLParsingException("Sub-queries are not supported: " + ctx.getText());
    }

    @Override
    public SObjectDescriptor.FieldDescriptor visitFunctionCall(SOQLParser.FunctionCallContext ctx) {
      throw new SOQLParsingException("Function calls are not supported: " + ctx.getText());
    }

    @Override
    public SObjectDescriptor.FieldDescriptor visitTypeOfClause(SOQLParser.TypeOfClauseContext ctx) {
      throw new SOQLParsingException("TypeOf clauses are not supported: " + ctx.getText());
    }
  }

}

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

package co.cask.hydrator.salesforce.parser;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import soql.SOQLLexer;
import soql.SOQLParser;

import java.util.ArrayList;
import java.util.List;

/**
 * Methods to get fields and sObject from salesforce query
 */
public class SalesforceQueryParser {
  /**
   * Get sObject from SOQL query
   * Example for "SELECT Id FROM Opportunity" it returns "Opportunity".
   * For nested queries returns top sObject
   *
   * @param query SOQL query
   *
   * @return sObject name
   */
  public static String getSObjectFromQuery(String query) {
    SOQLParser.StatementContext statementContext = getStatementContext(query);
    return statementContext.objectList().getText().toLowerCase(); // salesforce objects are case-insensitive
  }

  /**
   * Get field names from SOQL query
   * Example for "SELECT Id, Name FROM Opportunity" it returns ["Id", "Name"].
   *
   * @param query SOQL query
   * @return list of field names
   */
  public static List<String> getFieldsFromQuery(String query) {
    SOQLParser.StatementContext statementContext = getStatementContext(query);

    List<SOQLParser.FieldListContext> fieldsContextList = statementContext.fieldList();
    if (fieldsContextList.isEmpty()) {
      throw new SOQLParsingException("No fields were found in SOQL query. Query=" + query);
    }

    List<SOQLParser.FieldElementContext> fieldElementsContextList = statementContext.fieldList().get(0).fieldElement();

    List<String> fieldsList = new ArrayList<>();
    for (SOQLParser.FieldElementContext fieldElementsContext : fieldElementsContextList) {
      fieldsList.add(fieldElementsContext.getText());
    }

    return fieldsList;
  }

  private static SOQLParser.StatementContext getStatementContext(String query) {
    SOQLLexer lexer = new SOQLLexer(new ANTLRInputStream(query));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SOQLParser parser = new SOQLParser(tokens);
    parser.addErrorListener(new ThrowingErrorListener());
    return parser.statement();
  }

}

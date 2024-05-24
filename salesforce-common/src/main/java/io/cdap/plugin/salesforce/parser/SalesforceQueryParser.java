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
package io.cdap.plugin.salesforce.parser;

import io.cdap.plugin.salesforce.SObjectDescriptor;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import soql.SOQLLexer;
import soql.SOQLParser;

/**
 * Utility class that parses SOQL query.
 */
public class SalesforceQueryParser {

  /**
   * Parses given SOQL query and retrieves top sObject information and its fields information.
   *
   * @param query SOQL query
   * @return sObject descriptor
   */
  public static SObjectDescriptor getObjectDescriptorFromQuery(String query) {
    SOQLParser parser = initParser(query);
    SalesforceQueryVisitor visitor = new SalesforceQueryVisitor();
    return visitor.visit(parser.statement());
  }

  /**
   * Returns part of SOQL query after select statement.
   *
   * @param query SOQL query
   * @return from statement
   */
  public static String getFromStatement(String query) {
    SOQLParser parser = initParser(query);
    SalesforceQueryVisitor.FromStatementVisitor visitor = new SalesforceQueryVisitor.FromStatementVisitor();
    return visitor.visit(parser.statement());
  }

  /**
   * Checks if query has restricted syntax that cannot be processed by Bulk API.
   *
   * @param query SOQL query
   * @return true if query has restricted syntax, false otherwise
   */
  public static boolean isRestrictedQuery(String query) {
    SOQLParser parser = initParser(query);
    SalesforceQueryVisitor.RestrictedQueryVisitor visitor = new SalesforceQueryVisitor.RestrictedQueryVisitor();
    return visitor.visit(parser.statement());
  }

  /**
   * Checks if query has restricted syntax that cannot be processed by Bulk API with PK Enable.
   *
   * @param query SOQL query
   * @return true if query has restricted syntax, false otherwise
   */
  public static boolean isRestrictedPKQuery(String query) {
    SOQLParser parser = initParser(query);
    SalesforceQueryVisitor.RestrictedPKQueryVisitor visitor = new SalesforceQueryVisitor.RestrictedPKQueryVisitor();
    return visitor.visit(parser.statement());
  }

  private static SOQLParser initParser(String query) {
    SOQLLexer lexer = new SOQLLexer(CharStreams.fromString(query));
    lexer.removeErrorListeners();
    lexer.addErrorListener(ThrowingErrorListener.INSTANCE);

    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SOQLParser parser = new SOQLParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(ThrowingErrorListener.INSTANCE);

    return parser;
  }

  /**
   * Error listener which throws exception when parsing error happens, instead of just default writing to stderr.
   */
  private static class ThrowingErrorListener extends BaseErrorListener {

    static final ThrowingErrorListener INSTANCE = new ThrowingErrorListener();

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                            int line, int charPositionInLine, String msg, RecognitionException e) {
      StringBuilder builder = new StringBuilder();
      builder.append("Line [").append(line).append("]");
      builder.append(", position [").append(charPositionInLine).append("]");
      if (offendingSymbol != null) {
        builder.append(", offending symbol ").append(offendingSymbol);
      }
      if (msg != null) {
        builder.append(": ").append(msg);
      }
      throw new SOQLParsingException(builder.toString());
    }
  }

}

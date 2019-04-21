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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.plugin.salesforce.plugin.ErrorHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Based on pre-defined error strategy, handles errors which occur during batch source transform stage.
 */
public class ErrorHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ErrorHandler.class);

  private static final String ERROR_SCHEMA_BODY_PROPERTY = "body";

  private static final Schema ERROR_SCHEMA = Schema.recordOf("error",
    Schema.Field.of(ERROR_SCHEMA_BODY_PROPERTY, Schema.of(Schema.Type.STRING)));

  private final ErrorHandling errorHandling;

  public ErrorHandler(ErrorHandling errorHandling) {
    this.errorHandling = errorHandling;
  }

  public void handle(Emitter<StructuredRecord> emitter,
                     Map<String, String> value,
                     Exception exception) throws Exception {
    switch (errorHandling) {
      case SKIP:
        LOG.warn("Cannot process Salesforce row '{}', skipping it.", value, exception);
        break;
      case SEND:
        StructuredRecord.Builder builder = StructuredRecord.builder(ERROR_SCHEMA);
        builder.set(ERROR_SCHEMA_BODY_PROPERTY, value);
        emitter.emitError(new InvalidEntry<>(400, exception.getMessage(), builder.build()));
        break;
      case STOP:
        throw exception;
      default:
        throw new UnexpectedFormatException(
          String.format("Unknown error handling strategy '%s'", errorHandling));
    }
  }
}

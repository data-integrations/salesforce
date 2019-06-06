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
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.Map;

/**
 * Defines Salesforce function types based on resulting type and nullability.
 */
public enum SalesforceFunctionType {

  /**
   * Function resulting type and nullability is the same as field type and nullability.
   * Example: MAX(CreatedDate)
   */
  IDENTITY(false) {
    @Override
    public Schema getSchema(Schema fieldSchema) {
      return fieldSchema;
    }
  },

  /**
   * Function resulting type can be numeric (Long or Double), accepts only fields with type Int, Long or Double.
   * Nullability depends on field nullability.
   * Example: SUM(Amount)
   */
  NUMERIC(false) {
    @Override
    public Schema getSchema(Schema fieldSchema) {
      Schema.Type type = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      switch (type) {
        case INT:
        case LONG:
          return SalesforceFunctionType.addNullability(Schema.of(Schema.Type.LONG), fieldSchema.isNullable());
        case DOUBLE:
          return SalesforceFunctionType.addNullability(Schema.of(Schema.Type.DOUBLE), fieldSchema.isNullable());
        default:
          throw new IllegalArgumentException("Non numeric field schema type: " + type);
      }
    }
  },

  /**
   * Function resulting type is Double, nullability depends on field nullability.
   * Example: AVG(Amount)
   */
  DOUBLE(false) {
    @Override
    public Schema getSchema(Schema fieldSchema) {
      return SalesforceFunctionType.addNullability(Schema.of(Schema.Type.DOUBLE), fieldSchema.isNullable());
    }
  },

  /**
   * Function resulting type is Long, can not be null.
   * Example: COUNT(), COUNT(Id)
   */
  LONG_REQUIRED(true) {
    @Override
    public Schema getSchema(Schema fieldSchema) {
      return Schema.of(Schema.Type.LONG);
    }
  },

  /**
   * Function resulting type is Int, can not be null.
   * Example: GROUPING(LeadSource)
   */
  INT_REQUIRED(true) {
    @Override
    public Schema getSchema(Schema fieldSchema) {
      return Schema.of(Schema.Type.INT);
    }
  },

  /**
   * Function resulting type is Int, nullability depends on field nullability.
   * Example: CALENDAR_YEAR(CloseDate)
   */
  INT(false) {
    @Override
    public Schema getSchema(Schema fieldSchema) {
      return SalesforceFunctionType.addNullability(Schema.of(Schema.Type.INT), fieldSchema.isNullable());
    }
  },

  /**
   * Function resulting type is Date, nullability depends on field nullability.
   * Example: DAY_ONLY(CloseDate)
   */
  DATE(false) {
    @Override
    public Schema getSchema(Schema fieldSchema) {
      return SalesforceFunctionType.addNullability(Schema.of(Schema.LogicalType.DATE), fieldSchema.isNullable());
    }
  },

  /**
   * Function type is unknown,
   * presume that given field schema corresponds to function resulting type.
   */
  UNKNOWN(false) {
    @Override
    public Schema getSchema(Schema fieldSchema) {
      return fieldSchema;
    }
  },

  /**
   * Function call is absent, return initial field schema.
   */
  NONE(false) {
    @Override
    public Schema getSchema(Schema fieldSchema) {
      return fieldSchema;
    }
  };

  private boolean constant;

  SalesforceFunctionType(boolean constant) {
    this.constant = constant;
  }

  /**
   * Indicates if function type is constant,
   * i.e. if function resulting type and nullability depends on field characteristics.
   *
   * @return true if function type is constant, false otherwise
   */
  public boolean isConstant() {
    return constant;
  }

  public abstract Schema getSchema(Schema fieldSchema);

  private static Schema addNullability(Schema functionSchema, boolean nullable) {
    return nullable ? Schema.nullableOf(functionSchema) : functionSchema;
  }

  private static final Map<String, SalesforceFunctionType> SALESFORCE_FUNCTIONS_TYPE_MAPPING =
    new ImmutableMap.Builder<String, SalesforceFunctionType>()
      .put("MIN", SalesforceFunctionType.IDENTITY)
      .put("MAX", SalesforceFunctionType.IDENTITY)
      .put("SUM", SalesforceFunctionType.NUMERIC)
      .put("AVG", SalesforceFunctionType.DOUBLE)
      .put("COUNT", SalesforceFunctionType.LONG_REQUIRED)
      .put("COUNT_DISTINCT", SalesforceFunctionType.LONG_REQUIRED)
      .put("GROUPING", SalesforceFunctionType.INT_REQUIRED)
      .put("CALENDAR_MONTH", SalesforceFunctionType.INT)
      .put("CALENDAR_QUARTER", SalesforceFunctionType.INT)
      .put("CALENDAR_YEAR", SalesforceFunctionType.INT)
      .put("DAY_IN_MONTH", SalesforceFunctionType.INT)
      .put("DAY_IN_WEEK", SalesforceFunctionType.INT)
      .put("DAY_IN_YEAR", SalesforceFunctionType.INT)
      .put("FISCAL_MONTH", SalesforceFunctionType.INT)
      .put("FISCAL_QUARTER", SalesforceFunctionType.INT)
      .put("FISCAL_YEAR", SalesforceFunctionType.INT)
      .put("HOUR_IN_DAY", SalesforceFunctionType.INT)
      .put("WEEK_IN_MONTH", SalesforceFunctionType.INT)
      .put("WEEK_IN_YEAR", SalesforceFunctionType.INT)
      .put("DAY_ONLY", SalesforceFunctionType.DATE)
      .build();

  /**
   * Determines function type for the given function name. Case insensitive.
   *
   * @param functionName function name
   * @return function type
   */
  public static SalesforceFunctionType get(String functionName) {
    return SALESFORCE_FUNCTIONS_TYPE_MAPPING.getOrDefault(
      Preconditions.checkNotNull(functionName, "Function name can not be null").toUpperCase(), UNKNOWN);
  }
}

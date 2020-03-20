/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.math.expr;

import com.google.common.primitives.Ints;
import io.druid.java.util.common.Pair;
import io.druid.common.utils.StringUtils;
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.util.Objects;

/**
 */
public class ExprEval extends Pair<Object, ValueDesc>
{
  public static final ExprEval TRUE = ExprEval.of(true, ValueDesc.BOOLEAN);
  public static final ExprEval FALSE = ExprEval.of(false, ValueDesc.BOOLEAN);
  public static final ExprEval NULL_BOOL = ExprEval.of(null, ValueDesc.BOOLEAN);

  public static final ExprEval UNKNOWN = ExprEval.of(null, ValueDesc.UNKNOWN);

  public static ExprEval bestEffortOf(Object val)
  {
    return bestEffortOf(val, null);
  }

  public static ExprEval bestEffortOf(Object val, ValueDesc defaultType)
  {
    if (val == null) {
      return ExprEval.of(null, defaultType == null ? ValueDesc.STRING : defaultType);
    }
    if (val instanceof ExprEval) {
      return (ExprEval) val;
    }
    if (val instanceof Boolean) {
      return ExprEval.of(val, ValueDesc.BOOLEAN);
    }
    if (val instanceof String) {
      return ExprEval.of(val, ValueDesc.STRING);
    }
    if (val instanceof Number) {
      if (val instanceof BigDecimal) {
        return ExprEval.of(val, ValueDesc.ofDecimal((BigDecimal) val));
      }
      if (val instanceof Byte || val instanceof Short || val instanceof Integer || val instanceof Long) {
        return ExprEval.of(((Number) val).longValue());
      }
      if (val instanceof Float) {
        return ExprEval.of(((Number) val).floatValue());
      }
      return ExprEval.of(((Number) val).doubleValue());
    }
    if (val instanceof DateTime) {
      return ExprEval.of((DateTime) val);
    }
    return new ExprEval(val, defaultType == null ? ValueDesc.UNKNOWN : defaultType);
  }

  public static ExprEval of(Object value, ValueDesc type)
  {
    if (value instanceof ExprEval) {
      return (ExprEval) value;
    }
    return new ExprEval(value, type);
  }

  public static ExprEval of(Long longValue)
  {
    return of(longValue, ValueDesc.LONG);
  }

  public static ExprEval of(Integer intValue)
  {
    return of(intValue == null ? null : intValue.longValue(), ValueDesc.LONG);
  }

  public static ExprEval of(Float floatValue)
  {
    return of(floatValue, ValueDesc.FLOAT);
  }

  public static ExprEval of(Double doubleValue)
  {
    return of(doubleValue, ValueDesc.DOUBLE);
  }

  public static ExprEval of(BigDecimal decimal)
  {
    return of(decimal, ValueDesc.ofDecimal(decimal));
  }

  public static ExprEval of(DateTime dateTimeValue)
  {
    return of(dateTimeValue, ValueDesc.DATETIME);
  }

  public static ExprEval of(String stringValue)
  {
    return of(stringValue, ValueDesc.STRING);
  }

  public static ExprEval of(Boolean bool)
  {
    return of(bool, ValueDesc.BOOLEAN);
  }

  public static ExprEval of(boolean bool)
  {
    return bool ? TRUE : FALSE;
  }

  public ExprEval(Object lhs, ValueDesc rhs)
  {
    super(lhs, rhs);
  }

  public Object value()
  {
    return lhs;
  }

  public ValueDesc type()
  {
    return rhs;
  }

  public boolean isNull()
  {
    return StringUtils.isNullOrEmpty(lhs);
  }

  public boolean isPrimitive()
  {
    return rhs.isPrimitive();
  }

  public boolean isBoolean()
  {
    return rhs.isBoolean();
  }

  public boolean isString()
  {
    return rhs.isStringOrDimension();
  }

  public boolean isNumeric()
  {
    return rhs.isNumeric();
  }

  public boolean isLong()
  {
    return rhs.isLong();
  }

  public boolean isFloat()
  {
    return rhs.isFloat();
  }

  public boolean isDouble()
  {
    return rhs.isDouble();
  }

  public boolean isDecimal()
  {
    return rhs.isDecimal();
  }

  public boolean booleanValue()
  {
    return lhs != null && (Boolean) lhs;
  }

  public int intValue()
  {
    return lhs == null ? 0 : ((Number) lhs).intValue();
  }

  public long longValue()
  {
    return lhs == null ? 0L : ((Number) lhs).longValue();
  }

  public float floatValue()
  {
    return lhs == null ? 0F : ((Number) lhs).floatValue();
  }

  public double doubleValue()
  {
    return lhs == null ? 0D : ((Number) lhs).doubleValue();
  }

  public Number numberValue()
  {
    return (Number) lhs;
  }

  public String stringValue()
  {
    return (String) lhs;
  }

  public DateTime dateTimeValue()
  {
    return (DateTime) lhs;
  }

  public String asString()
  {
    return Objects.toString(lhs, null);
  }

  public DateTime asDateTime()
  {
    return Evals.toDateTime(this, (DateTimeZone) null);
  }

  public boolean asBoolean()
  {
    if (isNull()) {
      return false;
    } else if (rhs.isBoolean()) {
      return (Boolean) value();
    } else if (rhs.isNumeric()) {
      return doubleValue() > 0;
    } else if (rhs.isStringOrDimension()) {
      return Boolean.valueOf(stringValue());
    } else {
      return true;
    }
  }

  public Float asFloat()
  {
    if (isNull()) {
      return null;
    } else if (rhs.isBoolean()) {
      return (Boolean) value() ? 1F : 0F;
    } else if (rhs.isNumeric()) {
      return floatValue();
    } else if (rhs.isStringOrDimension()) {
      return Rows.tryParseFloat(asString());
    } else if (rhs.type() == ValueType.DATETIME) {
      return (float) dateTimeValue().getMillis();
    }
    return 0F;
  }

  public Double asDouble()
  {
    if (isNull()) {
      return null;
    } else if (rhs.isBoolean()) {
      return (Boolean) value() ? 1D : 0D;
    } else if (rhs.isNumeric()) {
      return doubleValue();
    } else if (rhs.isStringOrDimension()) {
      return Rows.tryParseDouble(asString());
    } else if (rhs.type() == ValueType.DATETIME) {
      return (double) dateTimeValue().getMillis();
    }
    return 0D;
  }

  public Long asLong()
  {
    if (isNull()) {
      return null;
    } else if (rhs.isBoolean()) {
      return (Boolean) value() ? 1L : 0L;
    } else if (rhs.isNumeric()) {
      return longValue();
    } else if (rhs.isStringOrDimension()) {
      return Rows.tryParseLong(asString());
    } else if (rhs.type() == ValueType.DATETIME) {
      return dateTimeValue().getMillis();
    }
    return 0L;
  }

  public Integer asInt()
  {
    if (isNull()) {
      return null;
    } else if (rhs.isNumeric()) {
      return intValue();
    } else if (rhs.isStringOrDimension()) {
      return Rows.tryParseInt(asString());
    } else if (rhs.type() == ValueType.DATETIME) {
      return Ints.checkedCast(dateTimeValue().getMillis());
    }
    return 0;
  }

  public ExprEval defaultValue()
  {
    switch (rhs.type()) {
      case BOOLEAN:
        return ExprEval.of(false);
      case FLOAT:
        return ExprEval.of(0F);
      case DOUBLE:
        return ExprEval.of(0D);
      case LONG:
        return ExprEval.of(0L);
      case STRING:
        return ExprEval.of((String) null);
      case DATETIME:
        return ExprEval.of((DateTime) null);
    }
    return ExprEval.of(null, ValueDesc.UNKNOWN);
  }
}

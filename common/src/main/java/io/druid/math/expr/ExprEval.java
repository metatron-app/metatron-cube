/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.math.expr;

import com.google.common.primitives.Ints;
import com.metamx.common.Pair;
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.util.Objects;

/**
 */
public class ExprEval extends Pair<Object, ValueDesc>
{
  public static final ExprEval UNKNOWN = ExprEval.of(null, ValueDesc.UNKNOWN);

  public static ExprEval bestEffortOf(Object val)
  {
    return bestEffortOf(val, ValueDesc.UNKNOWN);
  }

  public static ExprEval bestEffortOf(Object val, ValueDesc defaultType)
  {
    if (val == null) {
      return ExprEval.of(null, ValueDesc.STRING);  // compatibility
    }
    if (val instanceof ExprEval) {
      return (ExprEval) val;
    }
    if (val instanceof String) {
      return ExprEval.of(val, ValueDesc.STRING);
    }
    if (val instanceof Number) {
      if (val instanceof Byte || val instanceof Short || val instanceof Integer || val instanceof Long) {
        return ExprEval.of(((Number)val).longValue(), ValueDesc.LONG);
      }
      if (val instanceof Float) {
        return ExprEval.of(((Number)val).floatValue(), ValueDesc.FLOAT);
      }
      if (val instanceof BigDecimal) {
        return ExprEval.of(val, ValueDesc.ofDecimal((BigDecimal) val));
      }
      return ExprEval.of(((Number)val).doubleValue(), ValueDesc.DOUBLE);
    }
    if (val instanceof DateTime) {
      return ExprEval.of((DateTime)val);
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

  public static ExprEval of(long longValue)
  {
    return of(longValue, ValueDesc.LONG);
  }

  public static ExprEval of(float longValue)
  {
    return of(longValue, ValueDesc.FLOAT);
  }

  public static ExprEval of(double longValue)
  {
    return of(longValue, ValueDesc.DOUBLE);
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

  public static ExprEval of(boolean bool)
  {
    return of(bool ? 1L : 0L);
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
    return lhs == null || rhs == ValueDesc.STRING && stringValue().isEmpty();
  }

  public boolean isPrimitive()
  {
    return rhs.isPrimitive();
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
    if (rhs.isNumeric()) {
      return doubleValue() > 0;
    } else if (rhs.isStringOrDimension()) {
      return !isNull() && Boolean.valueOf(stringValue());
    } else {
      return !isNull();
    }
  }

  public float asFloat()
  {
    if (rhs.isNumeric()) {
      return floatValue();
    }
    switch (rhs.type()) {
      case STRING:
        return isNull() ? 0F : Rows.tryParseFloat(asString());
      case DATETIME:
        return isNull() ? 0F : dateTimeValue().getMillis();
    }
    return 0F;
  }

  public double asDouble()
  {
    if (rhs.isNumeric()) {
      return doubleValue();
    }
    switch (rhs.type()) {
      case STRING:
        return isNull() ? 0D : Rows.tryParseDouble(asString());
      case DATETIME:
        return isNull() ? 0D : dateTimeValue().getMillis();
    }
    return 0D;
  }

  public long asLong()
  {
    if (rhs.isNumeric()) {
      return longValue();
    }
    switch (rhs.type()) {
      case STRING:
        return isNull() ? 0L : Rows.tryParseLong(asString());
      case DATETIME:
        return isNull() ? 0L : dateTimeValue().getMillis();
    }
    return 0L;
  }

  public int asInt()
  {
    if (rhs.isNumeric()) {
      return intValue();
    }
    switch (rhs.type()) {
      case STRING:
        return isNull() ? 0 : Rows.tryParseInt(asString());
      case DATETIME:
        return isNull() ? 0 : Ints.checkedCast(dateTimeValue().getMillis());
    }
    return 0;
  }

  public ExprEval defaultValue()
  {
    switch (rhs.type()) {
      case FLOAT:
        return ExprEval.of(0F);
      case DOUBLE:
        return ExprEval.of(0D);
      case LONG:
        return ExprEval.of(0L);
      case STRING:
        return ExprEval.of((String)null);
      case DATETIME:
        return ExprEval.of((DateTime) null);
    }
    return ExprEval.of(null, ValueDesc.UNKNOWN);
  }
}

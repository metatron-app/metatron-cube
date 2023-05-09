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
import io.druid.common.utils.StringUtils;
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.java.util.common.Pair;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableFloat;
import org.apache.commons.lang.mutable.MutableLong;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.math.BigDecimal;
import java.util.Objects;

/**
 */
public class ExprEval extends Pair<Object, ValueDesc>
{
  public static final ExprEval TRUE = new ExprEval(true, ValueDesc.BOOLEAN)
  {
    @Override
    public boolean asBoolean() { return true;}
  };
  public static final ExprEval FALSE = new ExprEval(false, ValueDesc.BOOLEAN)
  {
    @Override
    public boolean asBoolean() { return false;}
  };
  public static final ExprEval NULL_BOOL = new ExprEval(null, ValueDesc.BOOLEAN)
  {
    @Override
    public boolean asBoolean() { return false;}
  };

  public static final ExprEval UNKNOWN = ExprEval.of(null, ValueDesc.UNKNOWN);

  public static final ExprEval NULL_STRING = ExprEval.of(null, ValueDesc.STRING);
  public static final ExprEval NULL_FLOAT = ExprEval.of(null, ValueDesc.FLOAT);
  public static final ExprEval NULL_DOUBLE = ExprEval.of(null, ValueDesc.DOUBLE);
  public static final ExprEval NULL_LONG = ExprEval.of(null, ValueDesc.LONG);
  public static final ExprEval NULL_DATETIME = ExprEval.of(null, ValueDesc.DATETIME);
  public static final ExprEval NULL_MAP = ExprEval.of(null, ValueDesc.MAP);

  public static ExprEval bestEffortOf(Object val)
  {
    return bestEffortOf(val, null);
  }

  public static ExprEval bestEffortOf(Object val, ValueDesc defaultType)
  {
    if (val == null) {
      return ExprEval.nullOf(defaultType);
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

  public static ExprEval nullOf(ValueDesc type)
  {
    if (type == null) {
      return NULL_STRING;
    }
    switch (type.type()) {
      case BOOLEAN:  return NULL_BOOL;
      case FLOAT:    return NULL_FLOAT;
      case LONG:     return NULL_LONG;
      case DOUBLE:   return NULL_DOUBLE;
      case STRING:   return NULL_STRING;
      case DATETIME: return NULL_DATETIME;
    }
    if (type.isMap()) {
      return NULL_MAP;
    }
    return new ExprEval(null, type);
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
    return longValue == null ? ExprEval.NULL_LONG : new ExprEval(longValue, ValueDesc.LONG);
  }

  public static ExprEval of(Integer intValue)
  {
    return intValue == null ? ExprEval.NULL_LONG : new ExprEval(intValue.longValue(), ValueDesc.LONG);
  }

  public static ExprEval of(Float floatValue)
  {
    return floatValue == null ? ExprEval.NULL_FLOAT : new ExprEval(floatValue, ValueDesc.FLOAT);
  }

  public static ExprEval of(Double doubleValue)
  {
    return doubleValue == null ? ExprEval.NULL_DOUBLE : new ExprEval(doubleValue, ValueDesc.DOUBLE);
  }

  public static ExprEval of(BigDecimal decimal)
  {
    return of(decimal, ValueDesc.DECIMAL);
  }

  public static ExprEval of(DateTime dateTimeValue)
  {
    return dateTimeValue == null ? ExprEval.NULL_DATETIME : new ExprEval(dateTimeValue, ValueDesc.DATETIME);
  }

  public static ExprEval of(Interval interval)
  {
    return of(interval, ValueDesc.INTERVAL);
  }

  public static ExprEval of(String stringValue)
  {
    return stringValue == null ? ExprEval.NULL_STRING : new ExprEval(stringValue, ValueDesc.STRING);
  }

  public static ExprEval of(Boolean bool)
  {
    return bool == null ? ExprEval.NULL_BOOL : of(bool.booleanValue());
  }

  public static ExprEval of(boolean bool)
  {
    return bool ? TRUE : FALSE;
  }

  private ExprEval(Object lhs, ValueDesc rhs)
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

  public boolean isString()
  {
    return rhs.isString() || rhs.isDimension();
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
    } else if (rhs.isString()) {
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
    } else if (rhs.isString()) {
      return Rows.tryParseFloat(asString());
    } else if (rhs.type() == ValueType.DATETIME) {
      return (float) dateTimeValue().getMillis();
    }
    return 0F;
  }

  public boolean asFloat(MutableFloat handover)
  {
    if (isNull()) {
      return false;
    }
    final float fv;
    if (rhs.isBoolean()) {
      fv = (Boolean) value() ? 1F : 0F;
    } else if (rhs.isNumeric()) {
      fv = floatValue();
    } else if (rhs.isString()) {
      fv = Rows.tryParseFloat(asString());
    } else if (rhs.type() == ValueType.DATETIME) {
      fv = (float) dateTimeValue().getMillis();
    } else {
      fv = 0F;
    }
    handover.setValue(fv);
    return true;
  }

  public Double asDouble()
  {
    if (isNull()) {
      return null;
    } else if (rhs.isBoolean()) {
      return (Boolean) value() ? 1D : 0D;
    } else if (rhs.isNumeric()) {
      return doubleValue();
    } else if (rhs.isString()) {
      return Rows.tryParseDouble(asString());
    } else if (rhs.type() == ValueType.DATETIME) {
      return (double) dateTimeValue().getMillis();
    }
    return 0D;
  }

  public boolean asDouble(MutableDouble handover)
  {
    if (isNull()) {
      return false;
    }
    final double dv;
    if (rhs.isBoolean()) {
      dv = (Boolean) value() ? 1D : 0D;
    } else if (rhs.isNumeric()) {
      dv = doubleValue();
    } else if (rhs.isString()) {
      dv = Rows.tryParseDouble(asString());
    } else if (rhs.type() == ValueType.DATETIME) {
      dv = (double) dateTimeValue().getMillis();
    } else {
      dv = 0d;
    }
    handover.setValue(dv);
    return true;
  }

  public Long asLong()
  {
    if (isNull()) {
      return null;
    } else if (rhs.isBoolean()) {
      return (Boolean) value() ? 1L : 0L;
    } else if (rhs.isNumeric()) {
      return longValue();
    } else if (rhs.isString()) {
      return Rows.tryParseLong(asString());
    } else if (rhs.type() == ValueType.DATETIME) {
      return dateTimeValue().getMillis();
    }
    return 0L;
  }

  public boolean asLong(MutableLong handover)
  {
    if (isNull()) {
      return false;
    }
    final long lv;
    if (rhs.isBoolean()) {
      lv = (Boolean) value() ? 1L : 0L;
    } else if (rhs.isNumeric()) {
      lv = longValue();
    } else if (rhs.isString()) {
      lv = Rows.tryParseLong(asString());
    } else if (rhs.type() == ValueType.DATETIME) {
      lv = dateTimeValue().getMillis();
    } else {
      lv = 0L;
    }
    handover.setValue(lv);
    return true;
  }

  public Integer asInt()
  {
    if (isNull()) {
      return null;
    } else if (rhs.isNumeric()) {
      return intValue();
    } else if (rhs.isString()) {
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
        return ExprEval.FALSE;
      case FLOAT:
        return ExprEval.of(0F);
      case DOUBLE:
        return ExprEval.of(0D);
      case LONG:
        return ExprEval.of(0L);
      case STRING:
        return ExprEval.NULL_STRING;
      case DATETIME:
        return ExprEval.NULL_DATETIME;
    }
    return this;
  }
}

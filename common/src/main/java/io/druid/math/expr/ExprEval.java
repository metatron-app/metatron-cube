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
import org.joda.time.DateTime;

import java.util.Objects;

/**
 */
public class ExprEval extends Pair<Object, ExprType>
{
  public static ExprEval bestEffortOf(Object val)
  {
    if (val instanceof Number) {
      if (val instanceof Byte || val instanceof Short || val instanceof Integer || val instanceof Long) {
        return ExprEval.of(val, ExprType.LONG);
      }
      if (val instanceof Float || val instanceof Double) {
        return ExprEval.of(val, ExprType.DOUBLE);
      }
    }
    if (val instanceof DateTime) {
      return ExprEval.of((DateTime)val);
    }
    return ExprEval.of(val == null ? null : String.valueOf(val), ExprType.STRING);
  }

  public static ExprEval bestEffortOf(Object val, ExprType type)
  {
    if (val instanceof Number) {
      if (val instanceof Byte || val instanceof Short || val instanceof Integer || val instanceof Long) {
        return ExprEval.of(val, ExprType.LONG);
      }
      if (val instanceof Float || val instanceof Double) {
        return ExprEval.of(val, ExprType.DOUBLE);
      }
    }
    if (val instanceof DateTime) {
      return ExprEval.of((DateTime)val);
    }
    return new ExprEval(val, type != null ? type : ExprType.STRING);
  }

  public static ExprEval of(Object value, ExprType type)
  {
    return new ExprEval(value, type);
  }

  public static ExprEval of(long longValue)
  {
    return of(longValue, ExprType.LONG);
  }

  public static ExprEval of(double longValue)
  {
    return of(longValue, ExprType.DOUBLE);
  }

  public static ExprEval of(DateTime dateTimeValue)
  {
    return of(dateTimeValue, ExprType.DATETIME);
  }

  public static ExprEval of(String stringValue)
  {
    return of(stringValue, ExprType.STRING);
  }

  public static ExprEval of(boolean bool)
  {
    return of(bool ? 1L : 0L);
  }

  public ExprEval(Object lhs, ExprType rhs)
  {
    super(lhs, rhs);
  }

  public Object value()
  {
    return lhs;
  }

  public ExprType type()
  {
    return rhs;
  }

  public boolean isNull()
  {
    return lhs == null || rhs == ExprType.STRING && stringValue().isEmpty();
  }

  public boolean isNumeric()
  {
    return rhs == ExprType.LONG || rhs == ExprType.DOUBLE;
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
    return Evals.toDateTime(this, null);
  }

  public boolean asBoolean()
  {
    switch (rhs) {
      case DOUBLE:
        return doubleValue() > 0;
      case LONG:
        return longValue() > 0;
      case STRING:
        return !isNull() && Boolean.valueOf(stringValue());
      case DATETIME:
        // ?
    }
    return false;
  }

  public float asFloat()
  {
    switch (rhs) {
      case DOUBLE:
      case LONG:
        return floatValue();
      case STRING:
        return isNull() ? 0F : Float.valueOf(asString());
      case DATETIME:
        return isNull() ? 0F : asDateTime().getMillis();
    }
    return 0F;
  }

  public double asDouble()
  {
    switch (rhs) {
      case DOUBLE:
      case LONG:
        return doubleValue();
      case STRING:
        return isNull() ? 0D : Double.valueOf(asString());
      case DATETIME:
        return isNull() ? 0D : asDateTime().getMillis();
    }
    return 0D;
  }

  public long asLong()
  {
    switch (rhs) {
      case DOUBLE:
      case LONG:
        return longValue();
      case STRING:
        return isNull() ? 0L : Long.valueOf(asString());
      case DATETIME:
        return isNull() ? 0L : asDateTime().getMillis();
    }
    return 0L;
  }

  public int asInt()
  {
    switch (rhs) {
      case DOUBLE:
      case LONG:
        return intValue();
      case STRING:
        return isNull() ? 0 : Integer.valueOf(asString());
      case DATETIME:
        return isNull() ? 0 : Ints.checkedCast(asDateTime().getMillis());
    }
    return 0;
  }

  public ExprEval defaultValue()
  {
    switch (rhs) {
      case DOUBLE:
        return ExprEval.of(0D);
      case LONG:
        return ExprEval.of(0L);
      case STRING:
        return ExprEval.of((String)null);
      case DATETIME:
        return ExprEval.of((DateTime)null);
    }
    return ExprEval.of((String)null);
  }
}

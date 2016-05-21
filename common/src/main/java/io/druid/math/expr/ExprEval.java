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

import com.metamx.common.Pair;

/**
 */
public class ExprEval extends Pair<Object, ExprType>
{
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

  public static ExprEval of(String stringValue)
  {
    return of(stringValue, ExprType.STRING);
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

  public boolean isNumeric()
  {
    return rhs == ExprType.LONG || rhs == ExprType.DOUBLE;
  }

  public int intValue()
  {
    return ((Number) lhs).intValue();
  }

  public long longValue()
  {
    return ((Number) lhs).longValue();
  }

  public float floatValue()
  {
    return ((Number) lhs).floatValue();
  }

  public double doubleValue()
  {
    return ((Number) lhs).doubleValue();
  }

  public Number numberValue()
  {
    return (Number) lhs;
  }

  public String stringValue()
  {
    return (String) lhs;
  }

  public String asString()
  {
    return lhs == null || rhs == ExprType.STRING ? stringValue() : String.valueOf(lhs);
  }

  public boolean asBoolean()
  {
    switch (rhs) {
      case DOUBLE:
        return doubleValue() > 0;
      case LONG:
        return longValue() > 0;
      case STRING:
        return Boolean.valueOf(stringValue());
    }
    return false;
  }

  public ExprEval defaultValue()
  {
    switch (rhs) {
      case DOUBLE:
        return ExprEval.of(0D);
      case LONG:
        return ExprEval.of(0L);
      case STRING:
        return ExprEval.of(null);
    }
    return ExprEval.of(null);
  }
}

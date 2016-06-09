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

import com.google.common.base.Function;
import com.google.common.base.Strings;
import io.druid.data.input.impl.DimensionSchema;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Objects;

/**
 */
public class Evals
{
  static final DateTimeFormatter defaultFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

  static boolean eq(ExprEval leftVal, ExprEval rightVal)
  {
    if (isSameType(leftVal, rightVal)) {
      return Objects.equals(leftVal.value(), rightVal.value());
    }
    if (isAllNumeric(leftVal, rightVal)) {
      return leftVal.doubleValue() == rightVal.doubleValue();
    }
    return false;
  }

  private static boolean isSameType(ExprEval leftVal, ExprEval rightVal)
  {
    return leftVal.type() == rightVal.type();
  }

  static boolean isAllNumeric(ExprEval left, ExprEval right)
  {
    return left.isNumeric() && right.isNumeric();
  }

  static boolean isAllString(ExprEval left, ExprEval right)
  {
    return left.type() == ExprType.STRING && right.type() == ExprType.STRING;
  }

  static String getConstantString(Expr arg)
  {
    if (!(arg instanceof StringExpr)) {
      throw new RuntimeException(arg + " is not constant string");
    }
    return arg.eval(null).stringValue();
  }

  static long getConstantLong(Expr arg)
  {
    if (!(arg instanceof LongExpr)) {
      throw new RuntimeException(arg + " is not constant long");
    }
    return arg.eval(null).longValue();
  }

  public static boolean asBoolean(Number x)
  {
    if (x == null) {
      return false;
    } else if (x instanceof Long) {
      return x.longValue() > 0;
    } else {
      return x.doubleValue() > 0;
    }
  }

  public static com.google.common.base.Function<Comparable, Number> asNumberFunc(DimensionSchema.ValueType type)
  {
    switch (type) {
      case FLOAT:
        return new Function<Comparable, Number>()
        {
          @Override
          public Number apply(Comparable input)
          {
            return input == null ? 0F : (Float) input;
          }
        };
      case LONG:
        return new Function<Comparable, Number>()
        {
          @Override
          public Number apply(Comparable input)
          {
            return input == null ? 0L : (Long) input;
          }
        };
      case STRING:
        return new Function<Comparable, Number>()
        {
          @Override
          public Number apply(Comparable input)
          {
            String string = (String) input;
            return Strings.isNullOrEmpty(string)
                   ? 0L
                   : StringUtils.isNumeric(string) ? Long.valueOf(string) : Double.valueOf(string);
          }
        };
    }
    throw new UnsupportedOperationException("Unsupported type " + type);
  }

  static DateTime toDateTime(ExprEval arg)
  {
    switch (arg.type()) {
      case STRING:
        String string = arg.stringValue();
        return StringUtils.isNumeric(string) ? new DateTime(Long.valueOf(string)) : defaultFormat.parseDateTime(string);
      default:
        return new DateTime(arg.longValue());
    }
  }
}

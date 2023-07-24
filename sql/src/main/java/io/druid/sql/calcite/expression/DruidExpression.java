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

package io.druid.sql.calcite.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Chars;
import io.druid.common.Cacheable;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.VirtualColumn;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Represents two kinds of expression-like concepts that native Druid queries support:
 *
 * (1) SimpleExtractions, which are direct column access, possibly with an extractionFn
 * (2) native Druid expressions.
 */
public class DruidExpression implements Cacheable
{
  // Must be sorted
  private static final char[] SAFE_CHARS = " ,._-;:(){}[]<>!@#$%^&*`~?/".toCharArray();

  static {
    Arrays.sort(SAFE_CHARS);
  }

  private final SimpleExtraction simpleExtraction;
  private final String expression;

  private DruidExpression(final SimpleExtraction simpleExtraction, final String expression)
  {
    this.simpleExtraction = simpleExtraction;
    this.expression = Preconditions.checkNotNull(expression);
  }

  // LOOKUP, REGEXP_EXTRACT
  public static DruidExpression of(final SimpleExtraction simpleExtraction, final String expression)
  {
    return new DruidExpression(simpleExtraction, expression);
  }

  public static DruidExpression fromColumn(final String column)
  {
    return new DruidExpression(SimpleExtraction.of(column, null), identifier(column));
  }

  public static DruidExpression fromExpression(final String expression)
  {
    return new DruidExpression(null, expression);
  }

  public static DruidExpression fromStringLiteral(final String n)
  {
    return new DruidExpression(null, stringLiteral(n));
  }

  public static DruidExpression fromNumericLiteral(final Number n, final SqlTypeName typeName)
  {
    return new DruidExpression(null, numberLiteral(n, typeName));
  }

  public static DruidExpression fromFunctionCall(final String functionName, final List<DruidExpression> args)
  {
    return fromExpression(functionCall(functionName, args));
  }

  public static DruidExpression numberLiteral(final long n)
  {
    return DruidExpression.fromExpression(String.valueOf(n));
  }

  public static String numberLiteral(final double n)
  {
    return String.valueOf(n);
  }

  public static String numberLiteral(final Number n, final SqlTypeName typeName)
  {
    if (n == null) {
      return nullLiteral();
    } else if (SqlTypeName.FLOAT == typeName) {
      return n.floatValue() + "F";
    } else if (SqlTypeName.DOUBLE == typeName) {
      return n.doubleValue() + "D";
    }
    String v = n instanceof BigDecimal ? ((BigDecimal) n).toPlainString() : n.toString();
    if (SqlTypeName.DECIMAL == typeName) {
      return v + 'B';
    }
    return v;
  }

  public static String stringLiteral(final String s)
  {
    return s == null ? nullLiteral() : "'" + escape(s) + "'";
  }

  public static String nullLiteral()
  {
    return "''";
  }

  public static String functionCall(final String functionName, final List<DruidExpression> args)
  {
    Preconditions.checkNotNull(functionName, "functionName");
    Preconditions.checkNotNull(args, "args");
    for (int i = 0; i < args.size(); i++) {
      Preconditions.checkNotNull(args.get(i), "arg #%s", i);
    }
    return functionCall(functionName, Iterables.transform(args, DruidExpression::getExpression));
  }

  public static String functionCall(String functionName, Iterable<String> expressions)
  {
    final Iterator<String> iterator = expressions.iterator();
    final StringBuilder builder = new StringBuilder(functionName).append('(');
    while (iterator.hasNext()) {
      builder.append(iterator.next());
      if (iterator.hasNext()) {
        builder.append(',');
      }
    }
    return builder.append(')').toString();
  }

  public static String functionCall(final String functionName, final DruidExpression... args)
  {
    return functionCall(functionName, Arrays.asList(args));
  }

  public static String identifier(final String s)
  {
    return StringUtils.isSimpleIdentifier(s) ? s : StringUtils.format("\"%s\"", escape(s));
  }

  private static String escape(final String s)
  {
    final StringBuilder escaped = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      if (Character.isLetterOrDigit(c) || Arrays.binarySearch(SAFE_CHARS, c) >= 0) {
        escaped.append(c);
      } else {
        escaped.append("\\u").append(BaseEncoding.base16().encode(Chars.toByteArray(c)));
      }
    }
    return escaped.toString();
  }

  public String getExpression()
  {
    return expression;
  }

  public boolean isDirectColumnAccess()
  {
    return simpleExtraction != null && simpleExtraction.getExtractionFn() == null;
  }

  public String getDirectColumn()
  {
    return Preconditions.checkNotNull(simpleExtraction.getColumn());
  }

  public boolean isSimpleExtraction()
  {
    return simpleExtraction != null;
  }

  public Expr parse(RowSignature rowSignature)
  {
    return Parser.parse(expression, rowSignature);
  }

  public SimpleExtraction getSimpleExtraction()
  {
    return Preconditions.checkNotNull(simpleExtraction);
  }

  public VirtualColumn toVirtualColumn(String name)
  {
    return new ExprVirtualColumn(expression, name);
  }

  public DruidExpression map(
      final Function<SimpleExtraction, SimpleExtraction> extractionMap,
      final Function<String, String> expressionMap
  )
  {
    return new DruidExpression(
        simpleExtraction == null ? null : extractionMap.apply(simpleExtraction),
        expressionMap.apply(expression)
    );
  }

  public DruidExpression nested(Object field)
  {
    if (isDirectColumnAccess()) {
      return DruidExpression.fromColumn(String.format("%s.%s", getDirectColumn(), field));
    }
    return DruidExpression.fromExpression(String.format("%s.\"%s\"", getExpression(), field));
  }

  public boolean isConstant()
  {
    return simpleExtraction == null && Evals.isConstant(Parser.parse(expression));
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(simpleExtraction).append(expression);
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DruidExpression that = (DruidExpression) o;
    return Objects.equals(simpleExtraction, that.simpleExtraction) &&
           Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(simpleExtraction, expression);
  }

  @Override
  public String toString()
  {
    return "DruidExpression{" +
           "simpleExtraction=" + simpleExtraction +
           ", expression='" + expression + '\'' +
           '}';
  }
}

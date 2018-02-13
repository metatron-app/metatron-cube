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

package io.druid.query.groupby.orderby;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.Pair;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.ExprType;
import io.druid.math.expr.Parser;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class WindowContext implements Expr.WindowContext
{
  public static WindowContext newInstance(Map<String, ExprType> expectedTypes)
  {
    return new WindowContext(null, null, null, expectedTypes);
  }

  public WindowContext on(List<String> partitionColumns, List<OrderByColumnSpec> orderingSpecs)
  {
    return new WindowContext(partitionColumns, orderingSpecs, partition, expectedTypes);
  }

  public WindowContext with(List<Row> partition)
  {
    return new WindowContext(partitionColumns, orderingSpecs, partition, expectedTypes);
  }

  private final List<String> partitionColumns;
  private final List<OrderByColumnSpec> orderingSpecs;
  private final List<Row> partition;
  private final int length;
  private final Map<String, ExprType> expectedTypes;
  private final Map<String, ExprEval> temporary;
  private int index;

  // redirected name for "_".. todo replace expression itself
  private transient String redirection;

  private WindowContext(
      List<String> partitionColumns,
      List<OrderByColumnSpec> orderingSpecs,
      List<Row> partition,
      Map<String, ExprType> expectedTypes
  )
  {
    this.partitionColumns = partitionColumns == null ? ImmutableList.<String>of() : partitionColumns;
    this.orderingSpecs = orderingSpecs == null ? ImmutableList.<OrderByColumnSpec>of() : orderingSpecs;
    this.partition = partition == null ? ImmutableList.<Row>of() : partition;
    this.length = partition == null ? 0 : partition.size();
    this.expectedTypes = expectedTypes;
    this.temporary = Maps.newHashMap();
  }

  public void evaluate(String[] sortedKeys, List<Evaluator> expressions)
  {
    boolean hasRedirection = false;
    for (Evaluator evaluator : expressions) {
      List<String> required = Parser.findRequiredBindings(evaluator.assigned);
      if (required.contains("_") || required.contains("#_")) {
        hasRedirection = true;
        break;
      }
    }
    if (hasRedirection) {
      for (Evaluator expression : expressions) {
        for (String sortedKey : expression.filter(sortedKeys)) {
          redirection = sortedKey;
          evaluate(expression);
        }
        redirection = null;
      }
    } else {
      evaluate(expressions);
    }
  }

  public void evaluate(List<Evaluator> expressions)
  {
    for (Evaluator expression : expressions) {
      evaluate(expression);
    }
  }

  private void evaluate(Evaluator expression)
  {
    final int[] window = expression.toEvalWindow(length);
    ExprEval eval = null;
    String outputName = expression.outputName(redirection);
    for (index = window[0]; index < window[1]; index++) {
      eval = expression.assigned.eval(this);
      if (!expression.temporary) {
        expectedTypes.put(outputName, eval.type());
        Map<String, Object> event = ((MapBasedRow) partition.get(index)).getEvent();
        event.put(outputName, eval.value());
      }
    }
    if (eval != null && expression.temporary) {
      temporary.put(outputName, eval);
      expectedTypes.put(outputName, eval.type());
    }
    Parser.reset(expression.assigned);
  }

  public List<OrderByColumnSpec> orderingSpecs()
  {
    return orderingSpecs;
  }

  @Override
  public List<String> partitionColumns()
  {
    return partitionColumns;
  }

  @Override
  public Collection<String> names()
  {
    return partition.get(index).getColumns();
  }

  @Override
  public Object get(String name)
  {
    if (name.startsWith("#")) {
      if ("#_".equals(name)) {
        return temporary.get("#" + redirection);
      }
      return temporary.get(name);
    }
    if (redirection != null && "_".equals(name)) {
      name = redirection;
    }
    return partition.get(index).getRaw(name);
  }

  @Override
  public ExprType type(String name)
  {
    if ("_".equals(name)) {
      name = redirection;
    } else if ("#_".equals(name)) {
      name = "#" + redirection;
    }
    return expectedTypes.get(name);
  }

  public void addType(String name, ExprType type)
  {
    expectedTypes.put(name, type);
  }

  @Override
  public Object get(final int index, final String name)
  {
    return index >= 0 && index < length ? partition.get(index).getRaw(name) : null;
  }

  @Override
  public Iterable<Object> iterator(final String name)
  {
    return Iterables.transform(partition, accessFunction(name));
  }

  @Override
  public Iterable<Object> iterator(final int startRel, final int endRel, final String name)
  {
    List<Row> target;
    if (startRel > endRel) {
      target = partition.subList(Math.max(0, index + endRel), Math.min(length, index + startRel + 1));
      target = Lists.reverse(target);
    } else {
      target = partition.subList(Math.max(0, index + startRel), Math.min(length, index + endRel + 1));
    }
    return Iterables.transform(target, accessFunction(name));
  }

  @Override
  public int size()
  {
    return length;
  }

  @Override
  public int index()
  {
    return index;
  }

  private Function<Row, Object> accessFunction(final String name)
  {
    return new Function<Row, Object>()
    {
      @Override
      public Object apply(Row input)
      {
        return input.getRaw(name);
      }
    };
  }

  static class Evaluator
  {
    public static Evaluator of(String condition, Pair<String, Expr> assign)
    {
      return new Evaluator(condition, assign.lhs, assign.rhs);
    }

    private final Matcher matcher;
    private final boolean temporary;
    private final String outputName;
    private final int[] window;
    private final Expr assigned;

    public Evaluator(String condition, String assignee, Expr assigned)
    {
      String[] splits = assignee.split(":");
      if (splits.length == 1) {
        outputName = splits[0];
        window = new int[0];
      } else if (splits.length == 2) {
        outputName = splits[0];
        window = new int[]{Integer.valueOf(splits[1])};
      } else {
        outputName = splits[0];
        window = new int[]{Integer.valueOf(splits[1]), Integer.valueOf(splits[2])};
      }
      this.assigned = assigned;
      this.temporary = outputName.startsWith("#");
      this.matcher = condition == null ? null : Pattern.compile(condition).matcher("");
    }

    private Iterable<String> filter(String[] columns)
    {
      if (matcher == null) {
        return Arrays.asList(columns);
      }
      return Iterables.filter(
          Arrays.asList(columns), new Predicate<String>()
          {
            @Override
            public boolean apply(String input)
            {
              return matcher.reset(input).find();
            }
          }
      );
    }

    private String outputName(String redirection)
    {
      if (outputName.equals("_")) {
        return redirection;
      }
      if (outputName.equals("#_")) {
        return "#" + redirection;
      }
      return outputName;
    }

    private int[] toEvalWindow(int limit)
    {
      if (window.length == 0) {
        return new int[]{0, limit};
      }
      int index = window[0];
      if (index < 0) {
        index = limit + index;
      }
      int start = index;
      if (start < 0 || start >= limit) {
        throw new IllegalArgumentException("invalid window start " + start + "/" + limit);
      }
      int end = start + 1;
      if (window.length > 1) {
        end = start + window[1];
      }
      if (end < 0 || end > limit) {
        throw new IllegalArgumentException("invalid window end " + end + "/" + limit);
      }
      if (start > end) {
        throw new IllegalArgumentException("invalid window " + start + " ~ " + end);
      }
      return new int[]{start, end};
    }
  }
}
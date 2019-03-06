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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.Pair;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Parser;
import io.druid.segment.column.Column;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class WindowContext extends TypeResolver.Abstract implements Expr.WindowContext
{
  public static WindowContext newInstance(List<String> groupByColumns, Map<String, ValueDesc> expectedTypes)
  {
    return new WindowContext(groupByColumns, expectedTypes);
  }

  public WindowContext on(List<String> partitionColumns, List<OrderByColumnSpec> orderingSpecs)
  {
    this.partitionColumns = partitionColumns;
    this.orderingSpecs = orderingSpecs;
    return this;
  }

  public WindowContext with(List<Row> partition)
  {
    this.partition = partition;
    this.length = partition.size();
    return this;
  }

  public static List<String> outputNames(List<Frame> frames, List<String> valueColumns)
  {
    List<String> outputNames = Lists.newArrayList();
    for (Frame frame : frames) {
      if (frame.outputName == null) {
        for (String valueColumn : valueColumns) {
          String outputName = frame.outputName(valueColumn);
          if (!outputName.startsWith("#")) {
            outputNames.add(outputName);
          }
        }
      } else if (!frame.outputName.startsWith("#")) {
        outputNames.add(frame.outputName);
      }
    }
    return outputNames;
  }

  private final List<String> groupByColumns;
  private final Map<String, ValueDesc> expectedTypes;
  private final Map<String, Object> temporary;

  private List<String> partitionColumns;
  private List<OrderByColumnSpec> orderingSpecs;
  private List<Row> partition;
  private int length;

  private int index;

  // redirected name for "_".. todo replace expression itself
  private transient String redirection;

  private WindowContext(
      List<String> groupByColumns,
      Map<String, ValueDesc> expectedTypes
  )
  {
    this.groupByColumns = groupByColumns == null ? ImmutableList.<String>of() : groupByColumns;
    this.expectedTypes = Maps.newHashMap(expectedTypes);
    this.temporary = Maps.newHashMap();
  }

  public List<Row> evaluate(Iterable<Frame> expressions, List<String> sortedKeys)
  {
    boolean hasRedirection = false;
    for (Frame frame : expressions) {
      List<String> required = Parser.findRequiredBindings(frame.assigned);
      if (required.contains("_") || required.contains("#_")) {
        hasRedirection = true;
        break;
      }
    }
    if (hasRedirection) {
      for (Frame expression : expressions) {
        for (String sortedKey : expression.filter(sortedKeys)) {
          redirection = sortedKey;
          _evaluate(expression);
        }
      }
    } else {
      evaluate(expressions);
    }
    // clear temporary contexts
    redirection = null;
    for (String temporaryKey : temporary.keySet()) {
      expectedTypes.remove(temporaryKey);
    }
    temporary.clear();
    return partition;
  }

  public void evaluate(Iterable<Frame> expressions)
  {
    for (Frame expression : expressions) {
      _evaluate(expression);
    }
  }

  private void _evaluate(Frame expression)
  {
    final int[] window = expression.toEvalWindow(length);
    final String outputName = expression.outputName(redirection);
    final boolean temporaryAssign = outputName.startsWith("#");

    ExprEval eval = null;
    for (index = window[0]; index < window[1]; index++) {
      eval = expression.assigned.eval(this);
      if (!temporaryAssign) {
        expectedTypes.put(outputName, eval.type());
        Map<String, Object> event = ((MapBasedRow) partition.get(index)).getEvent();
        event.put(outputName, eval.value());
      }
    }
    if (eval != null && temporaryAssign) {
      temporary.put(outputName, eval.value());
      expectedTypes.put(outputName, eval.type());
    }
    Parser.reset(expression.assigned);
  }

  public List<OrderByColumnSpec> orderingSpecs()
  {
    return orderingSpecs;
  }

  public List<String> groupByColumns()
  {
    return groupByColumns;
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
    if (name.equals(Column.TIME_COLUMN_NAME)) {
      return partition.get(index).getTimestampFromEpoch();
    }
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
  public ValueDesc resolve(String name)
  {
    if (name.equals(Column.TIME_COLUMN_NAME)) {
      return ValueDesc.LONG;
    }
    if ("_".equals(name)) {
      name = redirection;
    } else if ("#_".equals(name)) {
      name = "#" + redirection;
    }
    return expectedTypes.get(name);
  }

  public void addType(String name, ValueDesc type)
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

  @Override
  public String toString()
  {
    return "expectedTypes = " + expectedTypes + (redirection == null ? "" : ", redirection = " + redirection);
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

  static class Frame
  {
    public static Frame of(String condition, Pair<Expr, Expr> assign)
    {
      return new Frame(condition, assign.lhs, assign.rhs);
    }

    private final Matcher matcher;
    private final String outputName;
    private final int[] window;
    private final Expr assignee;
    private final Expr assigned;

    private Frame(String condition, Expr assignee, Expr assigned)
    {
      this.assignee = assignee;
      this.assigned = assigned;

      if (containsRedirect(assigned)) {
        outputName = null;
        window = new int[0];
      } else {
        Object eval = Evals.toAssigneeEval(assignee).value();
        if (eval instanceof Object[]) {
          Object[] windowed = (Object[]) eval;
          outputName = (String) windowed[0];
          window = new int[]{(Integer)windowed[1], (Integer)windowed[2]};
        } else if (eval instanceof String) {
          outputName = (String) eval;
          window = new int[0];
        } else {
          throw new IllegalArgumentException("invalid assignee expression " + assignee);
        }
      }
      this.matcher = condition == null ? null : Pattern.compile(condition).matcher("");
    }

    private boolean containsRedirect(Expr expr)
    {
      final List<String> required = Parser.findRequiredBindings(expr);
      return required.contains("_") || required.contains("#_");
    }

    private Iterable<String> filter(List<String> columns)
    {
      if (matcher == null) {
        return columns;
      }
      return Iterables.filter(
          columns, new Predicate<String>()
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
      if (outputName != null) {
        return outputName;
      }
      ImmutableMap<String, Object> overrides = ImmutableMap.<String, Object>of(
          "_", redirection,
          "#_", "#" + redirection
      );
      return Evals.toAssigneeEval(assignee, overrides).asString();
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

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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.IAE;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.Pair;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Parser;
import io.druid.segment.column.Column;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private final List<String> groupByColumns;
  private final Map<String, ValueDesc> expectedTypes;
  private final Map<String, Object> temporary;

  private final Set<String> assignedColumns;

  private List<String> partitionColumns;
  private List<OrderByColumnSpec> orderingSpecs;
  private List<Row> partition;
  private int length;

  private int index;

  private WindowContext(List<String> groupByColumns, Map<String, ValueDesc> expectedTypes)
  {
    this.groupByColumns = groupByColumns == null ? ImmutableList.<String>of() : groupByColumns;
    this.expectedTypes = Maps.newHashMap(expectedTypes);
    this.assignedColumns = Sets.newHashSet();
    this.temporary = Maps.newHashMap();
  }

  public List<Row> evaluate(Iterable<Frame> expressions)
  {
    for (Frame frame : expressions) {
      _evaluate(frame);
    }
    // clear temporary contexts
    for (String temporaryKey : temporary.keySet()) {
      expectedTypes.remove(temporaryKey);
    }
    temporary.clear();
    return partition;
  }

  private void _evaluate(Frame expression)
  {
    Parser.init(expression.expression);

    final int[] window = expression.toEvalWindow(length);
    final boolean temporaryAssign = expression.isTemporaryAssign();

    ExprEval eval = null;
    for (index = window[0]; index < window[1]; index++) {
      eval = expression.expression.eval(this);
      if (!temporaryAssign) {
        expectedTypes.put(expression.outputName, eval.type());
        Row.Updatable row = (Row.Updatable) partition.get(index);
        row.set(expression.outputName, eval.value());
      }
    }
    if (eval != null && temporaryAssign) {
      temporary.put(expression.outputName, eval.value());
      expectedTypes.put(expression.outputName, eval.type());
    }
    if (!temporaryAssign) {
      assignedColumns.add(expression.outputName);
    }
  }

  public List<String> getOutputColumns(List<String> valueColumns)
  {
    return GuavaUtils.dedupConcat(
        partitionColumns,
        assignedColumns,
        valueColumns
    );
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
      return temporary.get(name);
    }
    return partition.get(index).getRaw(name);
  }

  @Override
  public ValueDesc resolve(String name)
  {
    return Column.TIME_COLUMN_NAME.equals(name) ? ValueDesc.LONG : expectedTypes.get(name);
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
  public boolean hasMore()
  {
    return index < length - 1;
  }

  @Override
  public String toString()
  {
    return "expectedTypes = " + expectedTypes;
  }

  private static Iterable<String> filter(final List<String> columns, final Matcher matcher)
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

  static class FrameFactory
  {
    private final Matcher matcher;
    private final String expression;

    public FrameFactory(String condition, String expression)
    {
      this.matcher = condition == null ? null : Pattern.compile(condition).matcher("");
      this.expression = Preconditions.checkNotNull(expression);
    }

    public Iterable<Frame> intialize(final WindowContext context, List<String> redirections)
    {
      return Iterables.transform(WindowContext.filter(redirections, matcher), new Function<String, Frame>()
      {
        @Override
        public Frame apply(String redirection)
        {
          Map<String, String> mapping = ImmutableMap.<String, String>of("_", redirection, "#_", "#" + redirection);
          context.addType("#" + redirection, context.resolve(redirection, ValueDesc.UNKNOWN));
          Expr parsed = Parser.parse(expression, mapping, context);
          return Frame.of(Evals.splitAssign(parsed));
        }
      });
    }
  }

  static class Frame
  {
    public static Frame of(Pair<Expr, Expr> assign)
    {
      Object eval = Evals.toAssigneeEval(assign.lhs).value();
      String outputName;
      int[] window;
      if (eval instanceof Object[]) {
        Object[] windowed = (Object[]) eval;
        outputName = (String) windowed[0];
        window = new int[]{(Integer) windowed[1], (Integer) windowed[2]};
      } else if (eval instanceof String) {
        outputName = (String) eval;
        window = new int[0];
      } else {
        throw new IAE("invalid assignee expression %s", assign.lhs);
      }
      return new Frame(assign.rhs, outputName, window);
    }

    private final Expr expression;
    private final String outputName;
    private final int[] window;

    private Frame(Expr expression, String outputName, int[] window)
    {
      this.expression = expression;
      this.outputName = outputName;
      this.window = window;
    }

    private boolean isTemporaryAssign()
    {
      return outputName.startsWith("#");
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
        throw new IAE("invalid window start %s / %s", start, limit);
      }
      int end = start + 1;
      if (window.length > 1) {
        end = start + window[1];
      }
      if (end < 0 || end > limit) {
        throw new IAE("invalid window end  %s / %s", end, limit);
      }
      if (start > end) {
        throw new IAE("invalid window %s ~ %s", start, end);
      }
      return new int[]{start, end};
    }
  }
}

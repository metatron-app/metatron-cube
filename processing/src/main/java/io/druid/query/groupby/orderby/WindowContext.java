/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.query.groupby.orderby;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.Pair;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.java.util.common.IAE;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Parser;
import io.druid.segment.column.Column;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class WindowContext implements TypeResolver, Expr.WindowContext, Function<String, ValueDesc>
{
  public static final WindowContext UNKNOWN = new WindowContext(
      Arrays.<String>asList(),
      ImmutableMap.<String, ValueDesc>of()
  )
  {
    @Override
    public ValueDesc resolve(String name)
    {
      return ValueDesc.UNKNOWN;
    }
  };

  public static WindowContext newInstance(List<String> columns, Map<String, ValueDesc> expectedTypes)
  {
    return new WindowContext(columns, expectedTypes);
  }

  public WindowContext on(List<String> partitionColumns, List<OrderByColumnSpec> orderingSpecs)
  {
    this.partitionColumns = partitionColumns == null ? ImmutableList.<String>of() : partitionColumns;
    this.partitionColumnTypes = Lists.newArrayList(Lists.transform(this.partitionColumns, this));
    this.orderingSpecs = partitionColumns == null ? ImmutableList.<OrderByColumnSpec>of() : orderingSpecs;
    return this;
  }

  public WindowContext with(Object[] partitionKeys, List<Row> partition)
  {
    this.partitionKeys = partitionKeys;
    this.partition = partition;
    this.length = partition.size();
    return this;
  }

  private final List<String> columns; // to keep order
  private final List<String> initialColumns;
  private final Map<String, ValueDesc> assignedColumns;
  private final Map<String, ExprEval> temporaryColumns;

  private List<String> partitionColumns;
  private List<ValueDesc> partitionColumnTypes;
  private List<OrderByColumnSpec> orderingSpecs;

  private Object[] partitionKeys;
  private List<Row> partition;
  private int length;

  private int index;

  private WindowContext(List<String> columns, Map<String, ValueDesc> expectedTypes)
  {
    this.columns = Lists.newArrayList(columns);
    this.initialColumns = ImmutableList.copyOf(columns);
    this.assignedColumns = Maps.newHashMap(expectedTypes);
    this.temporaryColumns = Maps.newHashMap();
  }

  public List<Row> evaluate(Iterable<Frame> frames)
  {
    for (Frame frame : frames) {
      evaluate(frame);
    }
    temporaryColumns.clear();
    return partition;
  }

  private void evaluate(Frame frame)
  {
    Parser.init(frame.expression);

    final int[] window = frame.toEvalWindow(length);
    final boolean temporaryAssign = frame.isTemporaryAssign();

    ExprEval eval = null;
    for (index = window[0]; index < window[1]; index++) {
      eval = frame.expression.eval(this);
      if (!temporaryAssign) {
        assigned(frame.outputName, eval.type());
        Row.Updatable row = (Row.Updatable) partition.get(index);
        row.set(frame.outputName, eval.value());
      }
    }
    if (eval != null && temporaryAssign) {
      temporary(frame.outputName, eval);
    }
  }

  public List<String> getInputColumns()
  {
    return initialColumns;
  }

  public List<String> getOutputColumns()
  {
    return columns;
  }

  public List<String> getExcludedColumns()
  {
    return GuavaUtils.exclude(initialColumns, partitionColumns);
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

  public List<Object> partitionValues()
  {
    return Arrays.asList(partitionKeys);
  }

  public List<ValueDesc> partitionTypes()
  {
    return partitionColumnTypes;
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
    ExprEval temporary = name.startsWith("#") ? temporaryColumns.get(name) : null;
    if (temporary != null) {
      return temporary.value();
    }
    return partition.get(index).getRaw(name);
  }

  @Override
  public ValueDesc resolve(String name)
  {
    if (name.equals(Column.TIME_COLUMN_NAME)) {
      return ValueDesc.LONG;
    }
    ExprEval temporary = name.startsWith("#") ? temporaryColumns.get(name) : null;
    if (temporary != null) {
      return temporary.type();
    }
    return assignedColumns.get(name);
  }

  @Override
  public ValueDesc apply(String input)
  {
    return resolve(input);
  }

  public void assigned(String name, ValueDesc type)
  {
    if (assignedColumns.put(name, type) == null) {
      columns.add(name);
    }
  }

  public void temporary(String name, ExprEval eval)
  {
    Preconditions.checkArgument(name.startsWith("#"), "'%s' is not temporary variable", name);
    temporaryColumns.put(name, eval);
  }

  @Override
  public ExprEval evaluate(final int index, final Expr expr)
  {
    return index >= 0 && index < length ? expr.eval(partition.get(index)) : ExprEval.of(null, expr.returns());
  }

  @Override
  public Iterable<Object> iterator(final Expr expr)
  {
    return Iterables.transform(partition, row -> Evals.evalValue(expr, row));
  }

  @Override
  public Iterable<Object> iterator(final int startRel, final int endRel, final Expr expr)
  {
    List<Row> target;
    if (startRel > endRel) {
      target = partition.subList(Math.max(0, index + endRel), Math.min(length, index + startRel + 1));
      target = Lists.reverse(target);
    } else {
      target = partition.subList(Math.max(0, index + startRel), Math.min(length, index + endRel + 1));
    }
    return Iterables.transform(target, row -> Evals.evalValue(expr, row));
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
    return "expectedTypes = " + assignedColumns;
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

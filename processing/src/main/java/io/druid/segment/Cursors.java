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

package io.druid.segment;

import io.druid.data.ValueDesc;
import io.druid.math.expr.Expr;
import io.druid.query.UDTF;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.select.TableFunctionSpec;
import io.druid.segment.bitmap.BitSets;
import io.druid.segment.bitmap.IntIterable;
import io.druid.segment.bitmap.IntIterators;

import java.util.BitSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

public class Cursors
{
  public static Cursor explode(Cursor cursor, TableFunctionSpec tableFn)
  {
    return tableFn == null ? cursor : new LateralProvider(cursor, tableFn);
  }

  private static class LateralProvider extends Cursor.ExprSupport
  {
    private final Cursor delegated;
    private final UDTF function;
    private final ValueMatcher matcher;

    public LateralProvider(Cursor delegated, TableFunctionSpec tableFn)
    {
      this.delegated = delegated;
      this.function = UDTF.toFunction(delegated, tableFn);
      this.matcher = toMatcher(this, tableFn);
      prepare();
    }

    private static ValueMatcher toMatcher(Cursor wrapped, TableFunctionSpec tableFn)
    {
      DimFilter filter = tableFn.getFilter();
      if (filter != null) {
        return filter.toFilter(wrapped).makeMatcher(wrapped);
      }
      return ValueMatcher.TRUE;
    }

    @Override
    public ValueDesc resolve(String column)
    {
      return delegated.resolve(column);
    }

    @Override
    public Iterable<String> getColumnNames()
    {
      return delegated.getColumnNames();
    }

    @Override
    public IntFunction attachment(String name)
    {
      return delegated.attachment(name);
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return function.makeDimensionSelector(dimensionSpec);
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return function.makeObjectColumnSelector(columnName);
    }

    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return ColumnSelectors.asFloat(makeObjectColumnSelector(columnName));
    }

    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return ColumnSelectors.asDouble(makeObjectColumnSelector(columnName));
    }

    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return ColumnSelectors.asLong(makeObjectColumnSelector(columnName));
    }

    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
    {
      return delegated.makeMathExpressionSelector(expression);
    }

    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(Expr expression)
    {
      return delegated.makeMathExpressionSelector(expression);
    }

    @Override
    public ValueMatcher makePredicateMatcher(DimFilter filter)
    {
      return super.makePredicateMatcher(filter);    // bitmap is not valid with lateral view. stick to matcher
    }

    @Override
    public int size()
    {
      return -1;
    }

    @Override
    public long getStartTime()
    {
      return delegated.getStartTime();
    }

    @Override
    public long getRowTimestamp()
    {
      return delegated.getRowTimestamp();
    }

    @Override
    public int offset()
    {
      return delegated.offset();
    }

    @Override
    public void advanceWithoutMatcher()
    {
      function.advance();
    }

    @Override
    public void advance()
    {
      if (function.advance()) {
        while (!matcher.matches() && function.advance()) {
        }
      }
    }

    private void prepare()
    {
      if (function.prepare()) {
        while (!matcher.matches() && function.advance()) {
        }
      }
    }

    @Override
    public boolean isDone()
    {
      return function.isDone();
    }

    @Override
    public void reset()
    {
      delegated.reset();
      function.prepare();
    }

    @Override
    public ColumnSelectorFactory forAggregators()
    {
      return this;    // todo UDTF + VC?
    }

    @Override
    public ScanContext scanContext()
    {
      return delegated.scanContext();
    }
  }

  public static IntIterable wrap(Cursor cursor)
  {
    if (cursor.isDone()) {
      return IntIterable.EMPTY;
    }
    ScanContext context = cursor.scanContext();
    if (context.is(Scanning.FULL)) {
      return () -> null;
    }
    if (context.awareTargetRows()) {
      AtomicBoolean first = new AtomicBoolean(true);
      return () -> {
        if (!first.compareAndSet(true, false)) {
          cursor.reset();
        }
        return IntIterators.wrap(cursor);
      };
    }
    final BitSet rows = new BitSet();
    for (; !cursor.isDone(); cursor.advance()) {
      rows.set(cursor.offset());
    }
    return () -> BitSets.iterator(rows);
  }
}

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

import io.druid.common.guava.BufferRef;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Expr;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.select.TableFunctionSpec;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;

import java.util.List;

public class Cursors
{
  public static Cursor explode(Cursor cursor, TableFunctionSpec tableFn)
  {
    return tableFn == null ? cursor : new DimensionExploding(cursor, tableFn);
  }

  private static class DimensionExploding extends Cursor.ExprSupport
  {
    private final Cursor delegated;
    private final List<String> explodings;
    private final ValueMatcher matcher;

    private final DimensionSelector[] sources;
    private int limit;
    private int current;

    public DimensionExploding(Cursor delegated, TableFunctionSpec tableFn)
    {
      this.delegated = delegated;
      this.explodings = tableFn.getParameters();
      this.sources = explodings.stream()
                               .map(c -> delegated.makeDimensionSelector(DefaultDimensionSpec.of(c)))
                               .toArray(x -> new DimensionSelector[x]);
      if (GuavaUtils.containsAny(explodings, Filters.getDependents(tableFn.getFilter()))) {
        matcher = tableFn.getFilter().toFilter(this).makeMatcher(this);
      } else {
        matcher = ValueMatcher.TRUE;
      }
      _prepare();
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
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      if (dimensionSpec instanceof DefaultDimensionSpec) {
        final int index = explodings.indexOf(dimensionSpec.getDimension());
        if (index >= 0) {
          if (sources[index] instanceof DimensionSelector.WithRawAccess) {
            return new WithRawAccess((DimensionSelector.WithRawAccess) sources[index]);
          }
          return new SingleValued(sources[index]);
        }
      }
      return delegated.makeDimensionSelector(dimensionSpec);
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
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return delegated.makeObjectColumnSelector(columnName);
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
      return super.makePredicateMatcher(filter);    // bitmap is not valid with lateral view
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
      delegated.advanceWithoutMatcher();
    }

    @Override
    public void advance()
    {
      if (_advance()) {
        while (!matcher.matches() && _advance()) {
        }
      }
    }

    private boolean _advance()
    {
      if (++current < limit) {
        return true;
      }
      for (limit = 0, current = 0; limit == 0 && !delegated.isDone(); ) {
        delegated.advance();
        if (delegated.isDone()) {
          return false;
        }
        limit = sources[0].getRow().size();
      }
      return true;
    }

    @Override
    public boolean isDone()
    {
      return current >= limit;
    }

    @Override
    public void reset()
    {
      delegated.reset();
      _prepare();
    }

    private void _prepare()
    {
      for (current = 0, limit = sources[0].getRow().size(); !matcher.matches() && _advance(); ) {
      }
    }

    private class SingleValued extends DelegatedDimensionSelector implements DimensionSelector.SingleValued
    {
      public SingleValued(DimensionSelector delegate)
      {
        super(delegate);
      }

      @Override
      public final IndexedInts getRow()
      {
        return IndexedInts.from(super.getRow().get(current));
      }
    }

    private class WithRawAccess extends SingleValued implements DimensionSelector.WithRawAccess
    {
      private final DimensionSelector.WithRawAccess delegate;

      public WithRawAccess(DimensionSelector.WithRawAccess delegate)
      {
        super(delegate);
        this.delegate = delegate;
      }

      @Override
      public Dictionary getDictionary()
      {
        return delegate.getDictionary();
      }

      @Override
      public byte[] getAsRaw(int id)
      {
        return delegate.getAsRaw(id);
      }

      @Override
      public BufferRef getAsRef(int id)
      {
        return delegate.getAsRef(id);
      }
    }
  }
}

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

package io.druid.query;

import io.druid.common.guava.BufferRef;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.UOE;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.select.TableFunctionSpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.IndexedInts;

import java.util.List;
import java.util.stream.Stream;

public interface UDTF
{
  DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec);

  ObjectColumnSelector makeObjectColumnSelector(String column);

  boolean prepare();

  boolean advance();

  boolean isDone();

  static UDTF toFunction(Cursor delegated, TableFunctionSpec tableFn)
  {
    if ("explode".equalsIgnoreCase(tableFn.getOperation())) {
      final List<String> parameters = tableFn.getParameters();
      if (parameters.stream().map(c -> delegated.resolve(c, ValueDesc.UNKNOWN)).allMatch(t -> t.isDimension())) {
        return new DimensionsExplode(delegated, parameters);
      }
    }
    throw new UOE("not supported operation [%s]", tableFn.getOperation());
  }

  static class DimensionsExplode implements UDTF
  {
    private final Cursor delegated;
    private final List<String> explodings;
    private final DimensionSelector[] sources;

    private int limit;
    private int current;

    public DimensionsExplode(Cursor delegated, List<String> explodings)
    {
      this.delegated = delegated;
      this.explodings = explodings;
      this.sources = explodings.stream()
                               .map(c -> delegated.makeDimensionSelector(DefaultDimensionSpec.of(c)))
                               .toArray(x -> new DimensionSelector[x]);
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

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String column)
    {
      return delegated.makeObjectColumnSelector(column);
    }

    @Override
    public boolean prepare()
    {
      for (current = 0, limit = sources[0].getRow().size(); limit == 0; advance()) {
      }
      return current < limit;
    }

    @Override
    public boolean advance()
    {
      if (++current < limit) {
        return true;
      }
      for (current = limit = 0; limit == 0 && !delegated.isDone(); ) {
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

    private class SingleValued extends DimensionSelector.Delegated implements DimensionSelector.SingleValued
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

      @Override
      public boolean isUnique()
      {
        return delegate.isUnique();
      }

      @Override
      public Stream<String> values()
      {
        return delegate.values();
      }
    }
  }
}

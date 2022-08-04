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
package io.druid.segment.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.collections.spatial.search.Bound;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.incremental.SpatialDimensionRowTransformer;

/**
 */
public class SpatialFilter implements Filter
{
  private final String dimension;
  private final Bound bound;

  public SpatialFilter(String dimension, Bound bound)
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension");
    this.bound = Preconditions.checkNotNull(bound, "bound");
  }

  @Override
  public BitmapHolder getBitmapIndex(FilterContext context)
  {
    final ImmutableRTree index = context.indexSelector().getSpatialIndex(dimension);
    return index == null ? null : BitmapHolder.exact(
        DimFilters.union(context.bitmapFactory(), index.search(bound))
    );
  }

  @Override
  public ValueMatcher makeMatcher(
      MatcherContext context,
      ColumnSelectorFactory factory
  )
  {
    final Predicate predicate = toPredicate();
    final ObjectColumnSelector selector = factory.makeObjectColumnSelector(dimension);
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return predicate.apply(selector.get());
      }
    };
  }

  private Predicate toPredicate()
  {
    return new Predicate()
    {
      @Override
      public boolean apply(Object input)
      {
        if (input instanceof String) {
          final float[] coordinate = SpatialDimensionRowTransformer.decode((String) input);
          return bound.contains(coordinate);
        } else {
          return false;
        }
      }
    };
  }
}

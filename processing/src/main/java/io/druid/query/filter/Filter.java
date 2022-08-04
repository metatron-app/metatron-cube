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

package io.druid.query.filter;

import io.druid.math.expr.Expression;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.filter.Filters;
import io.druid.segment.filter.MatcherContext;
import io.druid.segment.filter.NotFilter;

import javax.annotation.Nullable;
import java.util.List;

/**
 */
public interface Filter extends Expression
{
  // bitmap based filter will be applied whenever it's possible
  @Nullable
  BitmapHolder getBitmapIndex(FilterContext context);

  default ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return makeMatcher(null, factory);
  }

  // used when bitmap filter cannot be applied
  ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory);

  abstract class MatcherOnly implements Filter
  {
    @Override
    public final BitmapHolder getBitmapIndex(FilterContext context)
    {
      return null;
    }
  }

  abstract class BitmapOnly implements Filter
  {
    @Override
    public final ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
    {
      throw new UnsupportedOperationException("value matcher");
    }
  }

  // factory
  class Factory implements Expression.Factory<Filter>
  {
    @Override
    public Filter or(List<Filter> children)
    {
      return Filters.or(children);
    }

    @Override
    public Filter and(List<Filter> children)
    {
      return Filters.and(children);
    }

    @Override
    public Filter not(Filter expression)
    {
      return new NotFilter(expression);
    }
  }
}

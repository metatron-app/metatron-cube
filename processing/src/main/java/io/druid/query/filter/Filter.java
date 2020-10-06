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

import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.math.expr.Expression;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.filter.Filters;
import io.druid.segment.filter.NotFilter;

import javax.annotation.Nullable;
import java.util.List;

/**
 */
public interface Filter extends Expression
{
  // bitmap based filter will be applied whenever it's possible
  @Nullable ImmutableBitmap getBitmapIndex(FilterContext context);

  // used when bitmap filter cannot be applied
  ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory);

  abstract class ValueOnly implements Filter
  {
    @Override
    public final ImmutableBitmap getBitmapIndex(FilterContext context)
    {
      return null;
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
